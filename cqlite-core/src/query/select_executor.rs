//! CQL SELECT Query Executor for Direct SSTable Access
//!
//! This module implements the REVOLUTIONARY query executor that can run
//! CQL SELECT statements directly on SSTable files without Cassandra.
//!
//! Features:
//! - Direct SSTable file scanning with predicate pushdown
//! - Streaming results for memory efficiency
//! - Parallel execution across multiple SSTable files
//! - Advanced aggregation with hash-based grouping
//! - Collection operations (list[index], map['key'])

use super::{
    result::{
        cql_type_to_data_type, ColumnInfo, QueryMetadata, QueryResult, QueryResultIterator,
        QueryRow, StreamingConfig,
    },
    select_ast::*,
    select_optimizer::{AggregationPlan, ExecutionStep, OptimizedQueryPlan, SSTablePredicate},
};
use crate::{
    parser::complex_types::ComplexTypeParser,
    schema::{CqlType, SchemaManager},
    storage::StorageEngine,
    types::{RowKey, Value},
    Error, Result, TableId,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// SELECT query executor for SSTable-based storage
#[derive(Debug)]
pub struct SelectExecutor {
    /// Schema manager for metadata
    _schema: Arc<SchemaManager>,
    /// Storage engine for SSTable access
    storage: Arc<StorageEngine>,
}

/// Query execution context
///
/// Pure bookkeeping for an in-flight query. Only used internally; the public
/// API surface is `SelectExecutor` itself.
#[derive(Debug)]
struct ExecutionContext {
    /// Current table being queried
    pub table_id: TableId,
    /// Column metadata
    pub columns: Vec<ColumnInfo>,
    /// Row count processed so far
    pub rows_processed: u64,
}

/// Aggregation state for GROUP BY operations
#[derive(Debug)]
struct AggregationState {
    /// Vector for grouping since Value doesn't implement Hash
    groups: Vec<(Vec<Value>, Vec<AggregateValue>)>,
    /// Memory usage tracking
    memory_usage_bytes: usize,
    /// Maximum memory limit
    memory_limit_bytes: usize,
}

/// Aggregate value accumulator
#[derive(Debug, Clone)]
enum AggregateValue {
    Count(u64),
    Sum(f64),
    Avg { sum: f64, count: u64 },
    Min(Value),
    Max(Value),
}

// ---------------------------------------------------------------------------
// Free helpers: pure functions that don't depend on `&self`. These were
// previously duplicated as `_static` methods on `SelectExecutor`; centralising
// them lets both the streaming background task and the synchronous executor
// share one implementation.
// ---------------------------------------------------------------------------

/// Split a `TableId` of the form `"keyspace.table"` into its parts.
///
/// If no dot is present, the whole name becomes the table component and the
/// keyspace is `None`.
fn parse_table_id(table_id: &TableId) -> (Option<String>, String) {
    let table_str = table_id.name();
    match table_str.rfind('.') {
        Some(dot) => (
            Some(table_str[..dot].to_string()),
            table_str[dot + 1..].to_string(),
        ),
        None => (None, table_str.to_string()),
    }
}

/// Compare two `Value`s for equality, including limited cross-type numeric
/// coercion (int↔bigint, int↔float, bigint↔float).
///
/// `Value` implements `PartialEq` natively but only matches identical variants;
/// we additionally treat the small set of cross-numeric cases that show up in
/// CQL predicates.
fn values_equal(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    // Only coerce when both operands are numeric — otherwise non-numeric
    // pairs (e.g. Text vs Integer) would spuriously compare equal via `as_f64`.
    if same_numeric_family(a, b) {
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return x == y;
        }
    }
    false
}

/// True when both `Value`s are numeric variants eligible for cross-type coercion.
fn same_numeric_family(a: &Value, b: &Value) -> bool {
    a.as_f64().is_some() && b.as_f64().is_some()
}

/// Compare two `Value`s for ordering, returning `Ordering::Equal` for
/// incomparable variants. Used by sorting/aggregation paths that historically
/// swallowed comparison errors via `unwrap_or(0)`.
fn compare_values_ordering(a: &Value, b: &Value) -> std::cmp::Ordering {
    try_compare_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
}

/// Compare two `Value`s for ordering, returning an error when the operand
/// types are not comparable. Preferred in WHERE-clause evaluation so users see
/// a real diagnostic rather than a silent equality.
///
/// Cross-type numerics are coerced via `f64` first; same-variant comparisons
/// fall back to `Value::partial_cmp`. We deliberately avoid `partial_cmp` for
/// non-matching variants because it stringifies and would produce surprising
/// orderings (e.g. `Text("9")` < `Text("10")` lexicographically).
fn try_compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if same_numeric_family(a, b) {
        if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
            return Ok(x.partial_cmp(&y).unwrap_or(Ordering::Equal));
        }
    }
    if std::mem::discriminant(a) == std::mem::discriminant(b) {
        return a.partial_cmp(b).ok_or_else(|| {
            Error::query_execution("Cannot compare incompatible types".to_string())
        });
    }
    log::debug!("Cannot compare {:?} with {:?}", a, b);
    Err(Error::query_execution(
        "Cannot compare incompatible types".to_string(),
    ))
}

/// Evaluate the SSTable predicate set against a single `QueryRow`.
///
/// Returns `Ok(true)` only if every predicate is satisfied. A missing column
/// causes the row to be rejected.
///
/// Exposed publicly so the Arrow Flight server can apply identical predicate
/// pushdown semantics to its merged rows (output parity with SELECT).
pub fn evaluate_predicates(row: &QueryRow, predicates: &[SSTablePredicate]) -> Result<bool> {
    use super::select_optimizer::SSTableFilterOp;
    for predicate in predicates {
        let Some(column_value) = row.values.get(&predicate.column) else {
            return Ok(false);
        };
        let matches = match &predicate.operation {
            SSTableFilterOp::Equal => predicate
                .values
                .first()
                .is_some_and(|v| values_equal(column_value, v)),
            SSTableFilterOp::In => predicate.values.contains(column_value),
            SSTableFilterOp::Range => {
                if predicate.values.len() < 2 {
                    false
                } else {
                    let lo = &predicate.values[0];
                    let hi = &predicate.values[1];
                    compare_values_ordering(column_value, lo).is_ge()
                        && compare_values_ordering(column_value, hi).is_le()
                }
            }
            // Single-bound clustering inequalities (Issue #788). A missing bound
            // rejects the row, mirroring the `Range` len-guard above.
            SSTableFilterOp::Gt => predicate
                .values
                .first()
                .is_some_and(|b| compare_values_ordering(column_value, b).is_gt()),
            SSTableFilterOp::Gte => predicate
                .values
                .first()
                .is_some_and(|b| compare_values_ordering(column_value, b).is_ge()),
            SSTableFilterOp::Lt => predicate
                .values
                .first()
                .is_some_and(|b| compare_values_ordering(column_value, b).is_lt()),
            SSTableFilterOp::Lte => predicate
                .values
                .first()
                .is_some_and(|b| compare_values_ordering(column_value, b).is_le()),
            SSTableFilterOp::Prefix => matches!(
                (column_value, predicate.values.first()),
                (Value::Text(s), Some(Value::Text(p))) if s.starts_with(p)
            ),
            SSTableFilterOp::BloomFilter => true, // already checked upstream
        };
        if !matches {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Build a `QueryRow` from a single `(RowKey, Value)` produced by storage scan,
/// applying optional projection and synthesising partition-key columns from the
/// raw key bytes when a schema is available.
///
/// Partition-key columns are never stored in the cell payload, so they are
/// reconstructed from the raw row key via the canonical
/// [`crate::storage::partition_key_codec::decode_partition_key_columns`] (the
/// same codec the write engine uses). This is the fix for Issue #586: the
/// previous decoder assumed a `u16` length prefix for every TEXT key, which is
/// only correct for composite components — a single-component TEXT partition key
/// is raw bytes, so its column was silently dropped from scan-built rows.
///
/// Returns `None` for tombstoned rows (so the caller can `continue`).
///
/// Exposed publicly so other readers (e.g. the Arrow Flight server's compaction
/// merge producer) can assemble rows identically to the SELECT path, guaranteeing
/// output parity. The `value` is expected to be a `Value::Map` of decoded
/// non-partition-key cells; partition-key columns are reconstructed from `key`.
pub fn build_row_from_scan(
    key: RowKey,
    value: Value,
    projection: &[String],
    schema: Option<&crate::schema::TableSchema>,
) -> Option<QueryRow> {
    // Suppress tombstoned rows from user-visible output. A row tombstone reaches
    // here as `Value::Tombstone` (Issue #505); before that change it was `Value::Null`.
    // Both must be suppressed identically so deleted rows never appear in query results.
    if matches!(value, Value::Null | Value::Tombstone(_)) {
        return None;
    }

    let mut row_values = HashMap::new();
    let project = |name: &str| projection.is_empty() || projection.iter().any(|p| p == name);

    if let Value::Map(map) = value {
        for (col_name, col_value) in map {
            if let Value::Text(name) = col_name {
                if project(&name) {
                    row_values.insert(name, col_value);
                }
            }
        }
        // Cassandra never serialises partition-key columns in the cell payload;
        // reconstruct them from the raw row key when the schema is known. We
        // decode through the canonical codec shared with the write engine so
        // single-component (raw bytes) and composite (`[u16 len][bytes][0x00]`)
        // keys are handled identically on both paths (Issue #586).
        if let Some(schema) = schema {
            match crate::storage::partition_key_codec::decode_partition_key_columns(&key.0, schema)
            {
                Ok(pk_columns) => {
                    for (name, value) in pk_columns {
                        if project(&name) {
                            row_values.insert(name, value);
                        }
                    }
                }
                // Surface — never silently swallow — a decode failure, so a
                // missing partition-key column can't ship invisibly (Issue #586).
                Err(e) => {
                    log::warn!(
                        "Failed to reconstruct partition-key columns from row key \
                         (len={} bytes) for {}.{}: {}",
                        key.0.len(),
                        schema.keyspace,
                        schema.table,
                        e
                    );
                }
            }
        }
    } else {
        // Non-map fallback: expose the raw value plus a debug-formatted id.
        row_values.insert("data".to_string(), value);
        if project("id") {
            row_values.insert("id".to_string(), Value::Text(format!("{:?}", key)));
        }
    }

    Some(QueryRow {
        values: row_values,
        key,
        metadata: Default::default(),
    })
}

/// Apply an `ArithmeticOperator` to two same-typed numeric `Value`s.
///
/// Behaviour matches the previous inline implementations: same-type only
/// (no implicit coercion), and division/modulo by zero are reported as
/// query-execution errors. Float division-by-zero (matching the original
/// runtime path) yields IEEE inf/NaN rather than an error.
fn eval_arithmetic(op: &ArithmeticOperator, left: Value, right: Value) -> Result<Value> {
    use ArithmeticOperator::*;
    macro_rules! int_op {
        ($a:expr, $b:expr, $ctor:expr) => {
            match op {
                Add => Ok($ctor($a + $b)),
                Subtract => Ok($ctor($a - $b)),
                Multiply => Ok($ctor($a * $b)),
                Divide => {
                    if $b == 0 {
                        Err(Error::query_execution("Division by zero".to_string()))
                    } else {
                        Ok($ctor($a / $b))
                    }
                }
                Modulo => {
                    if $b == 0 {
                        Err(Error::query_execution("Modulo by zero".to_string()))
                    } else {
                        Ok($ctor($a % $b))
                    }
                }
            }
        };
    }
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => int_op!(a, b, Value::Integer),
        (Value::BigInt(a), Value::BigInt(b)) => int_op!(a, b, Value::BigInt),
        (Value::Float(a), Value::Float(b)) => match op {
            Add => Ok(Value::Float(a + b)),
            Subtract => Ok(Value::Float(a - b)),
            Multiply => Ok(Value::Float(a * b)),
            Divide => Ok(Value::Float(a / b)),
            Modulo => Ok(Value::Float(a % b)),
        },
        _ => Err(Error::query_execution(
            "Incompatible types for arithmetic".to_string(),
        )),
    }
}

/// Build the GROUP BY key for `row`. With no GROUP BY, all rows hash into a
/// single `[Null]` bucket (global aggregation).
fn build_group_key(row: &QueryRow, group_by_columns: &[String]) -> Vec<Value> {
    if group_by_columns.is_empty() {
        return vec![Value::Null];
    }
    group_by_columns
        .iter()
        .map(|col| row.values.get(col).cloned().unwrap_or(Value::Null))
        .collect()
}

/// Locate the group matching `key` in `groups`, or push a fresh entry with
/// initial aggregator state. Returns the index into `groups`.
///
/// `Value` doesn't implement `Hash`, so groups live in a `Vec` and lookup is
/// linear. This is unchanged from the legacy implementation; switching to a
/// hash map would change result-row ordering for callers that rely on
/// insertion order.
fn find_or_init_group(
    groups: &mut Vec<(Vec<Value>, Vec<AggregateValue>)>,
    key: Vec<Value>,
    aggregates: &[super::select_optimizer::AggregateComputation],
) -> usize {
    if let Some(idx) = groups.iter().position(|(k, _)| k == &key) {
        return idx;
    }
    let initial: Vec<_> = aggregates
        .iter()
        .map(|c| match c.function {
            AggregateType::Count => AggregateValue::Count(0),
            AggregateType::Sum => AggregateValue::Sum(0.0),
            AggregateType::Avg => AggregateValue::Avg { sum: 0.0, count: 0 },
            AggregateType::Min => AggregateValue::Min(Value::Null),
            AggregateType::Max => AggregateValue::Max(Value::Null),
        })
        .collect();
    groups.push((key, initial));
    groups.len() - 1
}

/// Apply one row's contribution to a single aggregate accumulator.
///
/// COUNT(*) always increments; COUNT(col) only increments on non-null. SUM and
/// AVG ignore non-numeric values. MIN/MAX clone the value only when it
/// becomes the new extremum, sparing per-row clones in the common case.
fn update_aggregate(
    state: &mut AggregateValue,
    agg_comp: &super::select_optimizer::AggregateComputation,
    row: &QueryRow,
) {
    let is_star = agg_comp.column == "*";
    // Look up the column once; for COUNT(*) we don't need it.
    let value: Option<&Value> = if is_star {
        None
    } else {
        row.values.get(&agg_comp.column)
    };
    let is_null = !is_star && value.is_none_or(Value::is_null);

    match state {
        AggregateValue::Count(count) => {
            if is_star || !is_null {
                *count += 1;
            }
        }
        AggregateValue::Sum(sum) => {
            if let Some(v) = value.and_then(Value::as_f64) {
                *sum += v;
            }
        }
        AggregateValue::Avg { sum, count } => {
            if let Some(v) = value.and_then(Value::as_f64) {
                *sum += v;
                *count += 1;
            }
        }
        AggregateValue::Min(min_val) => {
            if let Some(v) = value {
                if !v.is_null()
                    && (min_val.is_null() || compare_values_ordering(v, min_val).is_lt())
                {
                    *min_val = v.clone();
                }
            }
        }
        AggregateValue::Max(max_val) => {
            if let Some(v) = value {
                if !v.is_null()
                    && (max_val.is_null() || compare_values_ordering(v, max_val).is_gt())
                {
                    *max_val = v.clone();
                }
            }
        }
    }
}

/// Materialize a single aggregation group into a `QueryRow`.
fn finalize_group(
    group_key: Vec<Value>,
    group_aggregates: Vec<AggregateValue>,
    agg_plan: &AggregationPlan,
) -> QueryRow {
    let mut row_values = HashMap::new();

    for (i, col) in agg_plan.group_by_columns.iter().enumerate() {
        if let Some(v) = group_key.get(i) {
            row_values.insert(col.clone(), v.clone());
        }
    }

    for (i, agg_comp) in agg_plan.aggregates.iter().enumerate() {
        let result_value = match &group_aggregates[i] {
            AggregateValue::Count(count) => Value::BigInt(*count as i64),
            AggregateValue::Sum(sum) => Value::Float(*sum),
            AggregateValue::Avg { sum, count } => {
                if *count > 0 {
                    Value::Float(sum / (*count as f64))
                } else {
                    Value::Null
                }
            }
            AggregateValue::Min(val) | AggregateValue::Max(val) => val.clone(),
        };
        row_values.insert(agg_comp.alias.clone(), result_value);
    }

    QueryRow {
        values: row_values,
        key: RowKey::new(vec![]),
        metadata: Default::default(),
    }
}

/// Constant-folding arithmetic. Same operand-type rules as `eval_arithmetic`,
/// plus BigInt support and per-operator error wording matching the legacy
/// implementation (e.g. `"Cannot add incompatible types"` and
/// `"Modulo only supported for integers"`).
fn const_arithmetic(op: &ArithmeticOperator, left: Value, right: Value) -> Result<Value> {
    use ArithmeticOperator::*;

    // Modulo's error wording is special: any non-integer combination must
    // report `"Modulo only supported for integers"` regardless of which side
    // is offending.
    if matches!(op, Modulo) {
        return match (left, right) {
            (Value::Integer(a), Value::Integer(b)) => {
                eval_arithmetic(op, Value::Integer(a), Value::Integer(b))
            }
            (Value::BigInt(a), Value::BigInt(b)) => {
                eval_arithmetic(op, Value::BigInt(a), Value::BigInt(b))
            }
            _ => Err(Error::query_execution(
                "Modulo only supported for integers".to_string(),
            )),
        };
    }

    let verb = match op {
        Add => "add",
        Subtract => "subtract",
        Multiply => "multiply",
        Divide => "divide",
        Modulo => unreachable!("handled above"),
    };

    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => {
            eval_arithmetic(op, Value::Integer(a), Value::Integer(b))
        }
        (Value::BigInt(a), Value::BigInt(b)) => {
            eval_arithmetic(op, Value::BigInt(a), Value::BigInt(b))
        }
        (Value::Float(a), Value::Float(b)) => {
            // Constant Float Divide rejects 0.0 (legacy behaviour); runtime
            // Float divide does not. Modulo on Float is rejected above.
            if matches!(op, Divide) && b == 0.0 {
                return Err(Error::query_execution("Division by zero".to_string()));
            }
            eval_arithmetic(op, Value::Float(a), Value::Float(b))
        }
        _ => Err(Error::query_execution(format!(
            "Cannot {} incompatible types",
            verb
        ))),
    }
}

/// Translate a CQL LIKE pattern (`%`, `_`) into an anchored regex.
fn like_pattern_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 4);
    out.push('^');
    for ch in pattern.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            _ => out.push(ch),
        }
    }
    out.push('$');
    out
}

/// Parse a CQL type string (e.g. `"list<int>"`, `"text"`) into a [`CqlType`].
///
/// Returns `None` when the type string cannot be parsed (unknown or malformed
/// types). Used to populate `ColumnInfo::cql_type` from the schema's string
/// representation, satisfying the no-heuristics mandate (Issue #28).
fn parse_cql_type_str(type_str: &str) -> Option<CqlType> {
    let parser = ComplexTypeParser::new();
    parser
        .parse_type(type_str)
        .ok()
        .map(|parsed| parsed.cql_type)
}

impl SelectExecutor {
    /// Create a new SELECT executor
    pub fn new(schema: Arc<SchemaManager>, storage: Arc<StorageEngine>) -> Self {
        Self {
            _schema: schema,
            storage,
        }
    }

    /// Execute an optimized query plan
    pub async fn execute(&self, plan: OptimizedQueryPlan) -> Result<QueryResult> {
        let table_id = if let Some(ref from_clause) = plan.statement.from_clause {
            self.extract_table_id(from_clause)?
        } else {
            // For queries without FROM clause (like SELECT 1), use a dummy table ID
            TableId::new("_dummy_")
        };

        let mut context = ExecutionContext {
            table_id,
            columns: self.get_result_columns(&plan.statement).await?,
            rows_processed: 0,
        };

        // Handle queries without FROM clause (like SELECT 1)
        if plan.statement.from_clause.is_none() {
            return self.execute_constant_query(&plan.statement, &context).await;
        }

        // Execute the plan step by step
        let mut intermediate_results = Vec::new();

        // If no execution steps are provided, add a default table scan
        let execution_steps = if plan.execution_steps.is_empty() {
            vec![ExecutionStep::SSTableScan {
                table: context.table_id.clone(),
                predicates: vec![],
                projection: context.columns.iter().map(|c| c.name.clone()).collect(),
            }]
        } else {
            plan.execution_steps.clone()
        };

        for step in &execution_steps {
            match step {
                ExecutionStep::SSTableScan {
                    table,
                    predicates,
                    projection,
                    ..
                } => {
                    let rows = self
                        .execute_sstable_scan(table, predicates, projection, &mut context)
                        .await?;
                    intermediate_results = rows;
                }
                ExecutionStep::Filter { expression, .. } => {
                    intermediate_results = self
                        .execute_filter(intermediate_results, expression, &mut context)
                        .await?;
                }
                ExecutionStep::Sort { order_by, .. } => {
                    intermediate_results = self
                        .execute_sort(intermediate_results, order_by, &mut context)
                        .await?;
                }
                ExecutionStep::Aggregate { plan: agg_plan, .. } => {
                    intermediate_results = self
                        .execute_aggregation(intermediate_results, agg_plan, &mut context)
                        .await?;
                }
                ExecutionStep::Limit { count, offset } => {
                    intermediate_results = self
                        .execute_limit(intermediate_results, *count, *offset, &mut context)
                        .await?;
                }
                ExecutionStep::Project { columns } => {
                    intermediate_results = self
                        .execute_projection(intermediate_results, columns, &mut context)
                        .await?;
                }
            }
        }

        let total_rows = intermediate_results.len() as u64;

        // CRITICAL FIX (Issue #129/#140): Populate metadata.columns for SELECT *
        // When SELECT * is used and no schema was found, context.columns is empty.
        // Fall back to inferring column names from the first row's HashMap keys.
        // IMPORTANT: Must be sorted alphabetically for deterministic JSON output (Issue #129)!
        let mut columns = context.columns;
        if columns.is_empty() && !intermediate_results.is_empty() {
            // Try to resolve schema to get proper CQL types (Issue #674).
            let schema_opt = if let Some(ref from_clause) = plan.statement.from_clause {
                if let Ok(table_id) = self.extract_table_id(from_clause) {
                    let (keyspace, table_name) = parse_table_id(&table_id);
                    self._schema
                        .find_schema_by_table(&keyspace, &table_name)
                        .await
                } else {
                    None
                }
            } else {
                None
            };

            let first_row = &intermediate_results[0];
            let mut col_names: Vec<_> = first_row.values.keys().collect();
            col_names.sort(); // Sort alphabetically for deterministic ordering (Issue #129)

            let table_name_for_meta = schema_opt
                .as_ref()
                .map(|s| format!("{}.{}", s.keyspace, s.table));

            for (idx, col_name) in col_names.iter().enumerate() {
                // Look up CQL type from schema; derive flat DataType from it (Issue #674).
                let cql_type_opt = schema_opt.as_ref().and_then(|schema| {
                    schema
                        .columns
                        .iter()
                        .find(|c| c.name.as_str() == col_name.as_str())
                        .and_then(|c| parse_cql_type_str(&c.data_type))
                });

                let data_type = cql_type_opt
                    .as_ref()
                    .map(cql_type_to_data_type)
                    .unwrap_or(crate::types::DataType::Text);

                let mut col_info = ColumnInfo {
                    name: (*col_name).clone(),
                    data_type,
                    nullable: true,
                    position: idx,
                    table_name: table_name_for_meta.clone(),
                    cql_type: None,
                };
                if let Some(cql_type) = cql_type_opt {
                    col_info = col_info.with_cql_type(cql_type);
                }
                columns.push(col_info);
            }
        }

        Ok(QueryResult {
            rows: intermediate_results,
            rows_affected: total_rows, // Use actual number of rows returned
            execution_time_ms: 0,      // Will be set by the engine
            metadata: crate::query::result::QueryMetadata {
                columns,
                total_rows: Some(total_rows),
                plan_info: None,
                performance: Default::default(),
                warnings: vec![],
            },
        })
    }

    /// Execute an optimized query plan with streaming results (Issue #280)
    ///
    /// Instead of materializing all rows in memory, this method returns a
    /// `QueryResultIterator` that yields rows incrementally via a bounded channel.
    /// This enables memory-efficient processing of large result sets.
    ///
    /// # Memory Budget
    ///
    /// With default `StreamingConfig::buffer_size` of 1024 rows and ~1KB avg row size:
    /// - Channel buffer: ~1MB in flight
    /// - Background task: minimal overhead
    /// - Total streaming overhead: ~1-2MB (well within 128MB target)
    ///
    /// # Limitations
    ///
    /// Currently supports:
    /// - SSTableScan with predicates (streaming)
    /// - Filter/Limit/Project (applied during scan)
    ///
    /// `LIMIT` (and `OFFSET`, when present in the plan) is enforced by the
    /// streaming producer (`execute_streaming_background`): it skips `OFFSET`
    /// matches and stops scanning once `count` rows have been sent, so a
    /// `LIMIT N` query yields exactly `N` rows without materializing the rest
    /// (Issue #581).
    ///
    /// For ORDER BY/GROUP BY/DISTINCT, falls back to full execution then streams results.
    pub async fn execute_streaming(
        &self,
        plan: OptimizedQueryPlan,
        config: StreamingConfig,
    ) -> Result<QueryResultIterator> {
        // Check if query requires full materialization (ORDER BY, GROUP BY, aggregates)
        if self.requires_materialization(&plan) {
            log::info!("Query requires materialization (ORDER BY/GROUP BY/aggregates), using execute-then-stream");
            return self.execute_and_stream(plan, config).await;
        }

        let table_id = if let Some(ref from_clause) = plan.statement.from_clause {
            self.extract_table_id(from_clause)?
        } else {
            // For queries without FROM clause (like SELECT 1), fall back to execute
            return self.execute_and_stream(plan, config).await;
        };

        let columns = self.get_result_columns(&plan.statement).await?;

        // Create bounded channel for backpressure
        let (tx, rx) = mpsc::channel(config.buffer_size);

        // Determine execution steps
        let execution_steps = if plan.execution_steps.is_empty() {
            vec![ExecutionStep::SSTableScan {
                table: table_id.clone(),
                predicates: vec![],
                projection: columns.iter().map(|c| c.name.clone()).collect(),
            }]
        } else {
            plan.execution_steps.clone()
        };

        // Clone what we need for the background task
        let storage = Arc::clone(&self.storage);
        let schema_manager = Arc::clone(&self._schema);
        let buffer_size = config.buffer_size;

        // Spawn background task to stream rows
        tokio::spawn(async move {
            if let Err(e) = Self::execute_streaming_background(
                storage,
                schema_manager,
                table_id,
                execution_steps,
                tx,
                buffer_size,
            )
            .await
            {
                log::error!("Streaming execution error: {}", e);
                // Error is logged; channel will close and consumer will see None
            }
        });

        // Create metadata for the iterator
        let metadata = QueryMetadata {
            columns,
            total_rows: None, // Unknown for streaming
            plan_info: None,
            performance: Default::default(),
            warnings: vec![],
        };

        Ok(QueryResultIterator::new(rx, metadata))
    }

    /// Check if query plan requires full materialization before streaming
    fn requires_materialization(&self, plan: &OptimizedQueryPlan) -> bool {
        for step in &plan.execution_steps {
            match step {
                ExecutionStep::Sort { .. } => return true,
                ExecutionStep::Aggregate { .. } => return true,
                _ => {}
            }
        }

        // Check for DISTINCT
        matches!(plan.statement.select_clause, SelectClause::Distinct(_))
    }

    /// Fallback: Execute query fully, then stream the results
    async fn execute_and_stream(
        &self,
        plan: OptimizedQueryPlan,
        config: StreamingConfig,
    ) -> Result<QueryResultIterator> {
        // Execute full query
        let result = self.execute(plan).await?;

        // Create channel to stream results
        let (tx, rx) = mpsc::channel(config.buffer_size);

        // Spawn task to send rows through channel
        tokio::spawn(async move {
            for row in result.rows {
                if tx.send(Ok(row)).await.is_err() {
                    break; // Consumer dropped
                }
            }
            // Channel closes automatically when tx drops
        });

        Ok(QueryResultIterator::new(rx, result.metadata))
    }

    /// Background task: Execute streaming scan and send rows through channel
    async fn execute_streaming_background(
        storage: Arc<StorageEngine>,
        schema_manager: Arc<SchemaManager>,
        _table_id: TableId,
        execution_steps: Vec<ExecutionStep>,
        tx: mpsc::Sender<Result<QueryRow>>,
        buffer_size: usize,
    ) -> Result<()> {
        // Issue #581: LIMIT/OFFSET must be enforced by the producer in the
        // streaming path. The `ExecutionStep::Limit` arm previously only logged a
        // message and relied on a consumer that never applied it, so
        // `execute_streaming` yielded the full result set regardless of LIMIT.
        // Extract the bound up front (steps are ordered with Limit after the scan)
        // and stop sending once it is satisfied — mirroring `execute_limit`
        // (drain OFFSET, then truncate to `count`) row-by-row so the producer
        // stops scanning early.
        let limit = execution_steps.iter().find_map(|step| match step {
            ExecutionStep::Limit { count, offset } => Some((*count, offset.unwrap_or(0))),
            _ => None,
        });
        let (limit_count, mut offset_remaining) = match limit {
            Some((count, offset)) => (Some(count), offset),
            None => (None, 0),
        };

        // A `LIMIT 0` means no rows can ever be sent; return before scanning.
        if limit_count == Some(0) {
            return Ok(());
        }

        let mut sent: u64 = 0;

        for step in &execution_steps {
            match step {
                ExecutionStep::SSTableScan {
                    table,
                    predicates,
                    projection,
                    ..
                } => {
                    let (keyspace, table_name) = parse_table_id(table);
                    let schema_opt = schema_manager
                        .find_schema_by_table(&keyspace, &table_name)
                        .await;

                    // Issue #790: pull rows lazily from a bounded streaming scan
                    // instead of materializing the full result `Vec`. The reader
                    // parses one entry at a time into this channel, so live heap
                    // stays bounded by `buffer_size` rather than O(result rows).
                    let mut scan_stream = storage
                        .scan_stream(table, None, None, schema_opt.as_ref(), buffer_size)
                        .await?;

                    while let Some(item) = scan_stream.recv().await {
                        let (key, value) = item?;
                        let Some(row) =
                            build_row_from_scan(key, value, projection, schema_opt.as_ref())
                        else {
                            continue;
                        };

                        if !evaluate_predicates(&row, predicates)? {
                            continue;
                        }

                        // Apply OFFSET: skip the first `offset_remaining` matches.
                        if offset_remaining > 0 {
                            offset_remaining -= 1;
                            continue;
                        }

                        // Send row through channel (with backpressure). Consumer drop ends the scan.
                        if tx.send(Ok(row)).await.is_err() {
                            return Ok(());
                        }
                        sent += 1;

                        // Apply LIMIT: stop scanning once `count` rows have been
                        // sent. Dropping `scan_stream` here signals the producer
                        // (via a closed channel) to stop parsing early.
                        if let Some(count) = limit_count {
                            if sent >= count {
                                return Ok(());
                            }
                        }
                    }
                }
                ExecutionStep::Limit { .. } => {
                    // Enforced inline during the scan above (see the limit bound
                    // extracted before the loop).
                }
                // Projection and predicate filtering are pushed into SSTableScan above.
                ExecutionStep::Project { .. } | ExecutionStep::Filter { .. } => {}
                _ => {
                    log::warn!("Streaming execution: skipping unsupported step {:?}", step);
                }
            }
        }

        Ok(())
    }

    /// Execute SSTable scan with predicate pushdown.
    ///
    /// Per-row work (build row, decode partition key, evaluate predicates) is
    /// handled by the free helpers `build_row_from_scan` and
    /// `evaluate_predicates`, which are shared with the streaming background
    /// task to keep the two execution paths in lockstep.
    async fn execute_sstable_scan(
        &self,
        table: &TableId,
        predicates: &[SSTablePredicate],
        projection: &[String],
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        const MAX_RESULTS: usize = 1_000_000;

        log::info!(
            "Executing SSTableScan: table=\"{}\", predicates={:?}",
            table,
            predicates
        );

        let (keyspace, table_name) = parse_table_id(table);
        let schema_opt = self
            ._schema
            .find_schema_by_table(&keyspace, &table_name)
            .await;

        match schema_opt.as_ref() {
            Some(schema) => log::info!(
                "Found schema for {}.{} with {} columns",
                schema.keyspace,
                schema.table,
                schema.columns.len()
            ),
            None => log::info!(
                "No schema found for {}.{}, proceeding without schema-aware parsing",
                keyspace.as_deref().unwrap_or("unknown"),
                table_name
            ),
        }

        let scan_results = self
            .storage
            .scan(table, None, None, None, schema_opt.as_ref())
            .await?;

        log::info!("Scan returned {} rows", scan_results.len());

        let mut results = Vec::new();
        for (key, value) in scan_results {
            context.rows_processed += 1;

            // build_row_from_scan returns None for tombstoned/null rows (Issue #191).
            let Some(row) = build_row_from_scan(key, value, projection, schema_opt.as_ref()) else {
                continue;
            };

            if evaluate_predicates(&row, predicates)? {
                results.push(row);
            }

            if results.len() > MAX_RESULTS {
                return Err(Error::query_execution(
                    "Result set too large, consider adding LIMIT".to_string(),
                ));
            }
        }

        Ok(results)
    }

    /// Execute filtering step
    async fn execute_filter(
        &self,
        rows: Vec<QueryRow>,
        filter_expr: &WhereExpression,
        context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let mut filtered_rows = Vec::new();

        for row in rows {
            if self.evaluate_where_expression(filter_expr, &row)? {
                filtered_rows.push(row);
            }
            context.rows_processed += 1;
        }

        Ok(filtered_rows)
    }

    /// Evaluate WHERE expression against a row
    fn evaluate_where_expression(&self, expr: &WhereExpression, row: &QueryRow) -> Result<bool> {
        match expr {
            WhereExpression::Comparison(comp) => self.evaluate_comparison(comp, row),
            WhereExpression::And(exprs) => {
                for expr in exprs {
                    if !self.evaluate_where_expression(expr, row)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            WhereExpression::Or(exprs) => {
                for expr in exprs {
                    if self.evaluate_where_expression(expr, row)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            WhereExpression::Not(expr) => Ok(!self.evaluate_where_expression(expr, row)?),
            WhereExpression::Parentheses(expr) => self.evaluate_where_expression(expr, row),
        }
    }

    /// Evaluate comparison expression. Operators that need a single right
    /// operand share one `evaluate` call; IN/LIKE/IS NULL fall through to
    /// their custom branches.
    fn evaluate_comparison(&self, comp: &ComparisonExpression, row: &QueryRow) -> Result<bool> {
        use ComparisonOperator::*;

        let left_value = self.evaluate_select_expression(&comp.left, row)?;

        // Fast path for null tests, which ignore the right side.
        match comp.operator {
            IsNull => return Ok(left_value.is_null()),
            IsNotNull => return Ok(!left_value.is_null()),
            _ => {}
        }

        match (&comp.operator, &comp.right) {
            (
                op @ (Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan
                | GreaterThanOrEqual),
                ComparisonRightSide::Value(right_expr),
            ) => {
                let right_value = self.evaluate_select_expression(right_expr, row)?;
                let result = match op {
                    Equal => values_equal(&left_value, &right_value),
                    NotEqual => !values_equal(&left_value, &right_value),
                    LessThan => try_compare_values(&left_value, &right_value)?.is_lt(),
                    LessThanOrEqual => try_compare_values(&left_value, &right_value)?.is_le(),
                    GreaterThan => try_compare_values(&left_value, &right_value)?.is_gt(),
                    GreaterThanOrEqual => try_compare_values(&left_value, &right_value)?.is_ge(),
                    _ => unreachable!("guarded by outer match"),
                };
                Ok(result)
            }
            (In, ComparisonRightSide::ValueList(value_exprs)) => {
                for value_expr in value_exprs {
                    let value = self.evaluate_select_expression(value_expr, row)?;
                    if left_value == value {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            (Like, ComparisonRightSide::Value(pattern_expr)) => {
                let pattern = self.evaluate_select_expression(pattern_expr, row)?;
                if let (Value::Text(text), Value::Text(pattern_str)) = (&left_value, &pattern) {
                    Ok(self.match_like_pattern(text, pattern_str))
                } else {
                    Ok(false)
                }
            }
            _ => Err(Error::query_execution(
                "Unsupported comparison operator".to_string(),
            )),
        }
    }

    /// Evaluate SELECT expression against a row
    fn evaluate_select_expression(&self, expr: &SelectExpression, row: &QueryRow) -> Result<Value> {
        match expr {
            SelectExpression::Column(col_ref) => {
                row.values.get(&col_ref.column).cloned().ok_or_else(|| {
                    Error::query_execution(format!("Column not found: {}", col_ref.column))
                })
            }
            SelectExpression::Literal(value) => Ok(value.clone()),
            SelectExpression::CollectionAccess(access) => {
                self.evaluate_collection_access(access, row)
            }
            SelectExpression::Arithmetic(arith) => {
                let left = self.evaluate_select_expression(&arith.left, row)?;
                let right = self.evaluate_select_expression(&arith.right, row)?;
                self.evaluate_arithmetic(&arith.operator, left, right)
            }
            SelectExpression::Aliased(expr, _) => self.evaluate_select_expression(expr, row),
            SelectExpression::Aggregate(_) => {
                // Aggregate expressions should not be evaluated at row level
                // They should only be processed during the aggregation step
                Err(Error::query_execution(
                    "Aggregate expressions should be processed during aggregation step, not row evaluation".to_string(),
                ))
            }
            SelectExpression::Function(_) => {
                // Function expressions not yet implemented
                Err(Error::query_execution(
                    "Function expressions not yet implemented".to_string(),
                ))
            }
        }
    }

    /// Evaluate collection access operations (`list[idx]`, `map['key']`,
    /// `value IN set_column`).
    fn evaluate_collection_access(
        &self,
        access: &CollectionAccessExpression,
        row: &QueryRow,
    ) -> Result<Value> {
        let lookup_column = |col: &ColumnRef| -> Result<&Value> {
            row.values
                .get(&col.column)
                .ok_or_else(|| Error::query_execution(format!("Column not found: {}", col.column)))
        };

        match access {
            CollectionAccessExpression::ListIndex(col_ref, index_expr) => {
                let list_value = lookup_column(col_ref)?;
                let index_value = self.evaluate_select_expression(index_expr, row)?;

                let (Value::List(list), Value::Integer(index)) = (list_value, &index_value) else {
                    return Err(Error::query_execution("Invalid list access".to_string()));
                };
                if *index >= 0 && (*index as usize) < list.len() {
                    Ok(list[*index as usize].clone())
                } else {
                    Ok(Value::Null)
                }
            }
            CollectionAccessExpression::MapKey(col_ref, key_expr) => {
                let map_value = lookup_column(col_ref)?;
                let key_value = self.evaluate_select_expression(key_expr, row)?;

                let Value::Map(map) = map_value else {
                    return Err(Error::query_execution("Invalid map access".to_string()));
                };
                Ok(map
                    .iter()
                    .find(|(k, _)| *k == key_value)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Value::Null))
            }
            CollectionAccessExpression::SetContains(col_ref, value_expr) => {
                let set_value = lookup_column(col_ref)?;
                let test_value = self.evaluate_select_expression(value_expr, row)?;

                let Value::Set(set) = set_value else {
                    return Err(Error::query_execution(
                        "Invalid set contains operation".to_string(),
                    ));
                };
                Ok(Value::Boolean(set.contains(&test_value)))
            }
        }
    }

    /// Evaluate arithmetic expressions on a (left, op, right) triple.
    ///
    /// Runtime arithmetic supports same-type Integer or Float operands. Mixed
    /// types or non-numeric operands return an error. (Constant-folding
    /// arithmetic additionally accepts BigInt — see
    /// `evaluate_constant_expression`.)
    fn evaluate_arithmetic(
        &self,
        op: &ArithmeticOperator,
        left: Value,
        right: Value,
    ) -> Result<Value> {
        match (&left, &right) {
            (Value::Integer(_), Value::Integer(_)) | (Value::Float(_), Value::Float(_)) => {
                eval_arithmetic(op, left, right)
            }
            _ => Err(Error::query_execution(
                "Incompatible types for arithmetic".to_string(),
            )),
        }
    }

    /// Simple LIKE pattern matching. The CQL pattern syntax (`%`, `_`) is
    /// translated by `like_pattern_to_regex` before compilation.
    fn match_like_pattern(&self, text: &str, pattern: &str) -> bool {
        regex::Regex::new(&like_pattern_to_regex(pattern))
            .map(|re| re.is_match(text))
            .unwrap_or(false)
    }

    /// Execute sorting step
    async fn execute_sort(
        &self,
        mut rows: Vec<QueryRow>,
        order_by: &OrderByClause,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        rows.sort_by(|a, b| {
            for item in &order_by.items {
                let a_val = self
                    .evaluate_select_expression(&item.expression, a)
                    .unwrap_or(Value::Null);
                let b_val = self
                    .evaluate_select_expression(&item.expression, b)
                    .unwrap_or(Value::Null);

                let ordering = match item.direction {
                    SortDirection::Ascending => compare_values_ordering(&a_val, &b_val),
                    SortDirection::Descending => compare_values_ordering(&b_val, &a_val),
                };
                if !ordering.is_eq() {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    /// Execute the aggregation step. Splits naturally into three phases:
    /// build group key, accumulate per-aggregate state, then finalize each
    /// group into a result row.
    async fn execute_aggregation(
        &self,
        rows: Vec<QueryRow>,
        agg_plan: &AggregationPlan,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        const PER_ROW_MEMORY_ESTIMATE_BYTES: usize = 100;
        const DEFAULT_AGGREGATION_MEMORY_LIMIT: usize = 512 * 1024 * 1024;

        let mut agg_state = AggregationState {
            groups: Vec::new(),
            memory_usage_bytes: 0,
            memory_limit_bytes: DEFAULT_AGGREGATION_MEMORY_LIMIT,
        };

        for row in rows {
            let group_key = build_group_key(&row, &agg_plan.group_by_columns);
            let group_index =
                find_or_init_group(&mut agg_state.groups, group_key, &agg_plan.aggregates);
            let group_aggregates = &mut agg_state.groups[group_index].1;

            for (i, agg_comp) in agg_plan.aggregates.iter().enumerate() {
                update_aggregate(&mut group_aggregates[i], agg_comp, &row);
            }

            agg_state.memory_usage_bytes += PER_ROW_MEMORY_ESTIMATE_BYTES;
            if agg_state.memory_usage_bytes > agg_state.memory_limit_bytes {
                return Err(Error::query_execution(
                    "Aggregation memory limit exceeded".to_string(),
                ));
            }
        }

        let result_rows = agg_state
            .groups
            .into_iter()
            .map(|(group_key, group_aggregates)| {
                finalize_group(group_key, group_aggregates, agg_plan)
            })
            .collect();

        Ok(result_rows)
    }

    /// Execute limit step (apply OFFSET then truncate to LIMIT).
    async fn execute_limit(
        &self,
        mut rows: Vec<QueryRow>,
        count: u64,
        offset: Option<u64>,
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let start_index = offset.unwrap_or(0) as usize;
        if start_index >= rows.len() {
            return Ok(Vec::new());
        }
        rows.drain(..start_index);
        rows.truncate(count as usize);
        Ok(rows)
    }

    /// Execute projection step
    async fn execute_projection(
        &self,
        rows: Vec<QueryRow>,
        columns: &[SelectExpression],
        _context: &mut ExecutionContext,
    ) -> Result<Vec<QueryRow>> {
        let mut projected_rows = Vec::new();

        for row in rows {
            let mut projected_values = HashMap::new();

            for (i, expr) in columns.iter().enumerate() {
                let value = self.evaluate_select_expression(expr, &row)?;
                let column_name = match expr {
                    SelectExpression::Column(col_ref) => col_ref.column.clone(),
                    SelectExpression::Aliased(_, alias) => alias.clone(),
                    _ => format!("col_{i}"),
                };
                projected_values.insert(column_name, value);
            }

            projected_rows.push(QueryRow {
                values: projected_values,
                key: RowKey::new(vec![]),
                metadata: Default::default(),
            });
        }

        Ok(projected_rows)
    }

    /// Execute a query without FROM clause (constant expressions like SELECT 1)
    async fn execute_constant_query(
        &self,
        statement: &SelectStatement,
        _context: &ExecutionContext,
    ) -> Result<QueryResult> {
        let mut values = HashMap::new();
        let mut columns = Vec::new();

        match &statement.select_clause {
            SelectClause::All => {
                return Err(Error::query_execution(
                    "SELECT * requires a FROM clause".to_string(),
                ));
            }
            SelectClause::Columns(expressions) | SelectClause::Distinct(expressions) => {
                for (i, expr) in expressions.iter().enumerate() {
                    let (value, column_name) = self.evaluate_constant_expression(expr)?;
                    let key = column_name.unwrap_or_else(|| format!("column_{}", i));
                    values.insert(key.clone(), value);
                    columns.push(ColumnInfo {
                        name: key,
                        data_type: crate::types::DataType::Text, // Constant expressions have no schema type
                        nullable: true,
                        position: i,
                        table_name: None, // No table for constant expressions
                        cql_type: None,
                    });
                }
            }
        }

        let row = QueryRow::with_values(RowKey::new(vec![1]), values);

        Ok(QueryResult {
            rows: vec![row],
            rows_affected: 1, // Constant queries return 1 row
            execution_time_ms: 0,
            metadata: crate::query::result::QueryMetadata {
                columns,
                total_rows: Some(1),
                plan_info: None,
                performance: crate::query::result::PerformanceMetrics::default(),
                warnings: Vec::new(),
            },
        })
    }

    /// Evaluate a constant expression (no table access needed).
    ///
    /// Accepts literals, aliases, and arithmetic over same-typed Integer,
    /// BigInt, or Float operands. Modulo is restricted to integers (matching
    /// the original behaviour). Error messages are kept verbatim from the
    /// legacy implementation so any callers asserting on them still pass.
    #[allow(clippy::only_used_in_recursion)]
    fn evaluate_constant_expression(
        &self,
        expr: &SelectExpression,
    ) -> Result<(Value, Option<String>)> {
        match expr {
            SelectExpression::Literal(value) => Ok((value.clone(), None)),
            SelectExpression::Aliased(inner_expr, alias) => {
                let (value, _) = self.evaluate_constant_expression(inner_expr)?;
                Ok((value, Some(alias.clone())))
            }
            SelectExpression::Arithmetic(arith) => {
                let (left_val, _) = self.evaluate_constant_expression(&arith.left)?;
                let (right_val, _) = self.evaluate_constant_expression(&arith.right)?;
                let result = const_arithmetic(&arith.operator, left_val, right_val)?;
                Ok((result, None))
            }
            _ => Err(Error::query_execution(
                "Expression type not supported in constant queries".to_string(),
            )),
        }
    }

    /// Extract a `TableId` from a FROM clause. Cassandra CQL has no JOINs, so
    /// either form (bare table or aliased table) yields the same result.
    fn extract_table_id(&self, from_clause: &FromClause) -> Result<TableId> {
        match from_clause {
            FromClause::Table(table_id) | FromClause::TableAlias(table_id, _) => {
                Ok(table_id.clone())
            }
        }
    }

    async fn get_result_columns(&self, statement: &SelectStatement) -> Result<Vec<ColumnInfo>> {
        let mut columns = Vec::new();

        match &statement.select_clause {
            SelectClause::All => {
                // For SELECT *, look up the schema to get column names and CQL types.
                // This is needed for streaming mode where we can't wait for the first row.
                if let Some(ref from_clause) = statement.from_clause {
                    let table_id = self.extract_table_id(from_clause)?;
                    let (keyspace_opt, table_name) = parse_table_id(&table_id);

                    // Look up schema from SchemaManager
                    if let Some(schema) = self
                        ._schema
                        .find_schema_by_table(&keyspace_opt, &table_name)
                        .await
                    {
                        // Collect all schema columns (sorted alphabetically for determinism)
                        let mut schema_cols: Vec<&crate::schema::Column> =
                            schema.columns.iter().collect();
                        schema_cols.sort_by_key(|c| c.name.as_str());

                        let keyspace_str = keyspace_opt.as_deref().unwrap_or("");
                        let table_name_str = format!("{}.{}", keyspace_str, table_name);

                        for (idx, schema_col) in schema_cols.iter().enumerate() {
                            // Parse the CQL type string into a structured CqlType (Issue #674).
                            let cql_type_opt = parse_cql_type_str(&schema_col.data_type);
                            // Derive the flat DataType from the CqlType; avoids hardcoded Text.
                            let data_type = cql_type_opt
                                .as_ref()
                                .map(cql_type_to_data_type)
                                .unwrap_or(crate::types::DataType::Text);

                            let mut col_info = ColumnInfo {
                                name: schema_col.name.clone(),
                                data_type,
                                nullable: true,
                                position: idx,
                                table_name: Some(table_name_str.clone()),
                                cql_type: None,
                            };
                            if let Some(cql_type) = cql_type_opt {
                                col_info = col_info.with_cql_type(cql_type);
                            }
                            columns.push(col_info);
                        }

                        log::debug!(
                            "SELECT * resolved {} columns from schema for {:?}.{}",
                            columns.len(),
                            keyspace_opt,
                            table_name
                        );
                    }
                    // If schema not found, columns stay empty - will be populated from first row at runtime
                }
            }
            SelectClause::Columns(exprs) | SelectClause::Distinct(exprs) => {
                // Try to resolve a schema for the FROM table (if present) so we can
                // attach authoritative CQL types to explicitly projected columns (Issue #674).
                let schema_opt = if let Some(ref from_clause) = statement.from_clause {
                    if let Ok(table_id) = self.extract_table_id(from_clause) {
                        let (keyspace_opt, table_name) = parse_table_id(&table_id);
                        self._schema
                            .find_schema_by_table(&keyspace_opt, &table_name)
                            .await
                    } else {
                        None
                    }
                } else {
                    None
                };

                for (i, expr) in exprs.iter().enumerate() {
                    let column_name = match expr {
                        SelectExpression::Column(col_ref) => col_ref.column.clone(),
                        SelectExpression::Aliased(_, alias) => alias.clone(),
                        _ => format!("col_{i}"),
                    };

                    // Look up CQL type for this column in the schema (Issue #674).
                    let cql_type_opt = schema_opt.as_ref().and_then(|schema| {
                        schema
                            .columns
                            .iter()
                            .find(|c| c.name == column_name)
                            .and_then(|c| parse_cql_type_str(&c.data_type))
                    });
                    let data_type = cql_type_opt
                        .as_ref()
                        .map(cql_type_to_data_type)
                        .unwrap_or(crate::types::DataType::Text);

                    let mut col_info = ColumnInfo {
                        name: column_name,
                        data_type,
                        nullable: true,
                        position: i,
                        table_name: None,
                        cql_type: None,
                    };
                    if let Some(cql_type) = cql_type_opt {
                        col_info = col_info.with_cql_type(cql_type);
                    }
                    columns.push(col_info);
                }
            }
        }

        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{platform::Platform, Config};
    use tempfile::TempDir;

    async fn create_test_executor() -> SelectExecutor {
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
        let _schema = Arc::new(SchemaManager::new(temp_dir.path()).await.unwrap());

        SelectExecutor { _schema, storage }
    }

    #[test]
    fn test_value_comparison() {
        use std::cmp::Ordering;
        assert_eq!(
            try_compare_values(&Value::Integer(5), &Value::Integer(3)).unwrap(),
            Ordering::Greater
        );
        assert_eq!(
            try_compare_values(&Value::Integer(3), &Value::Integer(5)).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            try_compare_values(&Value::Integer(5), &Value::Integer(5)).unwrap(),
            Ordering::Equal
        );
    }

    #[tokio::test]
    async fn test_like_pattern_matching() {
        let executor = create_test_executor().await;

        assert!(executor.match_like_pattern("hello", "h%"));
        assert!(executor.match_like_pattern("hello", "%lo"));
        assert!(executor.match_like_pattern("hello", "h_llo"));
        assert!(!executor.match_like_pattern("hello", "h_l"));
    }

    // ------------------------------------------------------------------
    // Issue #586: partition-key reconstruction on the scan path.
    // ------------------------------------------------------------------

    fn single_pk_schema(name: &str, data_type: &str) -> crate::schema::TableSchema {
        crate::schema::TableSchema {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_keys: vec![crate::schema::KeyColumn {
                name: name.to_string(),
                data_type: data_type.to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![],
            comments: std::collections::HashMap::new(),
        }
    }

    /// Issue #586: a single-component TEXT partition key is stored as raw bytes
    /// with NO length prefix. `build_row_from_scan` must materialise it from the
    /// `RowKey`. Before the fix the column was silently dropped (the decoder
    /// read a phantom `u16` prefix, errored, and the error was swallowed).
    #[test]
    fn build_row_from_scan_materialises_single_text_pk() {
        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = Value::Map(vec![(
            Value::Text("name".to_string()),
            Value::Text("name-0".to_string()),
        )]);
        let schema = single_pk_schema("id", "text");

        let row = build_row_from_scan(key, value, &[], Some(&schema))
            .expect("row must be built (not tombstoned)");

        assert_eq!(
            row.values.get("id"),
            Some(&Value::Text("k0000000000000000".to_string())),
            "Issue #586: single TEXT PK column must be reconstructed from the raw row key"
        );
        // Regular columns must still be present.
        assert_eq!(
            row.values.get("name"),
            Some(&Value::Text("name-0".to_string()))
        );
    }

    /// Issue #586: with the PK column materialised, a residual `WHERE id = '...'`
    /// (the path TEXT single-PK queries fall through to) now matches.
    #[test]
    fn scan_built_row_matches_text_pk_equality_predicate() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let key = RowKey::new(b"k0000000000000000".to_vec());
        let value = Value::Map(vec![(Value::Text("age".to_string()), Value::Integer(0))]);
        let schema = single_pk_schema("id", "text");
        let row = build_row_from_scan(key, value, &[], Some(&schema)).unwrap();

        let predicate = SSTablePredicate {
            column: "id".to_string(),
            operation: SSTableFilterOp::Equal,
            values: vec![Value::Text("k0000000000000000".to_string())],
        };

        assert!(
            evaluate_predicates(&row, std::slice::from_ref(&predicate)).unwrap(),
            "Issue #586: WHERE id = '<literal>' must match the reconstructed PK column"
        );
    }

    /// Build a one-column `QueryRow` for predicate-evaluation tests.
    fn row_with_int(column: &str, value: i64) -> QueryRow {
        let mut values = std::collections::HashMap::new();
        values.insert(column.to_string(), Value::Integer(value as i32));
        QueryRow {
            values,
            key: RowKey::new(Vec::new()),
            metadata: Default::default(),
        }
    }

    /// Issue #788: each clustering-key inequality op must include/exclude rows on
    /// the correct side of its bound when evaluated post-scan.
    #[test]
    fn inequality_predicates_apply_single_bound() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let bound = |op: SSTableFilterOp| SSTablePredicate {
            column: "ck".to_string(),
            operation: op,
            values: vec![Value::Integer(200)],
        };
        let eval = |op: SSTableFilterOp, ck: i64| {
            evaluate_predicates(&row_with_int("ck", ck), std::slice::from_ref(&bound(op))).unwrap()
        };

        // ck > 200
        assert!(!eval(SSTableFilterOp::Gt, 200));
        assert!(eval(SSTableFilterOp::Gt, 201));
        // ck >= 200
        assert!(eval(SSTableFilterOp::Gte, 200));
        assert!(!eval(SSTableFilterOp::Gte, 199));
        // ck < 200
        assert!(eval(SSTableFilterOp::Lt, 199));
        assert!(!eval(SSTableFilterOp::Lt, 200));
        // ck <= 200
        assert!(eval(SSTableFilterOp::Lte, 200));
        assert!(!eval(SSTableFilterOp::Lte, 201));
    }

    /// Issue #788: the `pk = ? AND ck >= 0 AND ck < 200` shape — a two-bound AND
    /// set — must include `[0, 199]` and exclude `200`/`1000`, reproducing the
    /// 200-row slice the issue expects (previously the whole partition leaked).
    #[test]
    fn two_bound_inequality_slice_selects_half_open_range() {
        use super::super::select_optimizer::{SSTableFilterOp, SSTablePredicate};

        let predicates = vec![
            SSTablePredicate {
                column: "ck".to_string(),
                operation: SSTableFilterOp::Gte,
                values: vec![Value::Integer(0)],
            },
            SSTablePredicate {
                column: "ck".to_string(),
                operation: SSTableFilterOp::Lt,
                values: vec![Value::Integer(200)],
            },
        ];
        let in_slice = |ck: i64| evaluate_predicates(&row_with_int("ck", ck), &predicates).unwrap();

        assert!(in_slice(0), "lower bound is inclusive");
        assert!(in_slice(199), "last row in [0, 200) is included");
        assert!(!in_slice(200), "upper bound is exclusive");
        assert!(!in_slice(1000), "rows past the slice are excluded");
        assert!(!in_slice(-1), "rows below the slice are excluded");
    }
}
