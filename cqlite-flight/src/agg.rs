//! Server-side aggregation pushdown (issue #841).
//!
//! When a [`FlightTicket`](crate::ticket::FlightTicket) carries an
//! [`Aggregation`], the producer computes PARTIAL aggregates over exactly the
//! rows a `SELECT` would emit — token-pruned (#839), predicate-filtered (#834),
//! tombstone-suppressed and LWW-reconciled — and returns one partial row per
//! group instead of the full row set. The Java connector merges those partials
//! across ranges/nodes.
//!
//! # Partial output schema
//!
//! `[ group_by columns (in order) , aggregate `output` columns (in order) ]`.
//!
//! - group_by columns keep their normal mapped Arrow type (the column's
//!   `CqlType`, reusing [`schema_columns`](crate::producer::schema_columns) and
//!   the shared `arrow_convert` mapping).
//! - `Count` → `Int64`, never null.
//! - `Sum` → `Int64` for an integer-family source (TinyInt/SmallInt/Int/BigInt/
//!   Counter), `Float64` for Float/Double; null when the group has no non-null
//!   inputs. Any other source type is a [`AggError`] (the connector never pushes
//!   `Sum` on them).
//! - `Min`/`Max` → the source column's Arrow type; null when no non-null inputs.
//!
//! # Row semantics
//!
//! - Empty `group_by` (global): exactly one row always, even over zero input
//!   rows (`count(*) = 0`, `count(col) = 0`, sum/min/max null).
//! - Non-empty `group_by`: one row per distinct group key among surviving rows
//!   (a NULL/absent key value forms its own SQL-style group); zero rows on zero
//!   input.

use std::collections::HashMap;

use cqlite_core::query::{ColumnInfo, QueryRow};
use cqlite_core::schema::{CqlType, TableSchema};
use cqlite_core::types::{DataType, Value};
use cqlite_core::RowKey;

use crate::producer::schema_columns;
use crate::ticket::{AggFunc, AggregateSpec, Aggregation};

/// Errors building or running an aggregation.
#[derive(Debug, thiserror::Error)]
pub enum AggError {
    /// A group_by or aggregate referenced a column not in the schema.
    #[error("aggregation references unknown column '{0}'")]
    UnknownColumn(String),
    /// `Sum`/`Min`/`Max` named no source column (only `Count` may omit it).
    #[error("aggregate '{output}' ({func:?}) requires a source column")]
    MissingColumn {
        /// The offending output column name.
        output: String,
        /// The function that needs a column.
        func: AggFunc,
    },
    /// `Sum` was requested on a non-numeric source column.
    #[error("sum('{column}') unsupported: source type {cql_type:?} is not numeric")]
    SumNotNumeric {
        /// Source column name.
        column: String,
        /// The non-numeric source type.
        cql_type: CqlType,
    },
    /// An integer `Sum` exceeded `i64`. We surface an error rather than wrap so
    /// the result matches Trino's non-pushed `sum(bigint)` (which overflows hard)
    /// instead of silently returning a wrong value.
    #[error("sum('{column}') overflowed i64")]
    SumOverflow {
        /// Source column name.
        column: String,
    },
    /// `Min`/`Max` on a float/double column is not pushed: NaN ordering must match
    /// Trino exactly (tracked in a follow-up). The connector already declines this,
    /// so this guards a hand-crafted ticket and keeps the server consistent.
    #[error("min/max('{column}') unsupported: float/double NaN ordering is not pushed")]
    MinMaxFloatUnsupported {
        /// Source column name.
        column: String,
    },
}

/// Numeric family of a source column, deciding `Sum` output typing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SumKind {
    /// Integer family → `Int64` sum.
    Integer,
    /// Float/Double → `Float64` sum.
    Float,
}

/// A fully-validated aggregate: its function, resolved source column (if any),
/// the source column's `CqlType`, and the output column name.
#[derive(Debug, Clone)]
struct PlannedAggregate {
    func: AggFunc,
    column: Option<String>,
    /// Source column type — `None` only for `count(*)`.
    source_type: Option<CqlType>,
    /// For `Sum`, the numeric family of the source (decides Int64 vs Float64).
    sum_kind: Option<SumKind>,
    output: String,
}

/// A validated aggregation plan: ordered group-by columns (with types) and the
/// ordered planned aggregates. Built once per producer via [`AggPlan::build`].
#[derive(Debug, Clone)]
pub struct AggPlan {
    /// `(column name, CqlType)` for each group-by column, in order.
    group_by: Vec<(String, CqlType)>,
    aggregates: Vec<PlannedAggregate>,
}

/// Resolve a column's `CqlType` from any of the schema's column lists.
fn column_type(schema: &TableSchema, column: &str) -> Result<CqlType, AggError> {
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
        .ok_or_else(|| AggError::UnknownColumn(column.to_string()))?;
    // A type that fails to parse here is the same kind of bad-schema condition
    // the column lookup guards against; surface it as an unknown column rather
    // than introducing a new variant for an internally-built DDL.
    CqlType::parse(type_str).map_err(|_| AggError::UnknownColumn(column.to_string()))
}

/// Unwrap `Frozen(inner)` (transparent for aggregation typing).
fn unwrap_frozen(ty: &CqlType) -> &CqlType {
    match ty {
        CqlType::Frozen(inner) => unwrap_frozen(inner),
        other => other,
    }
}

/// The numeric family of a CQL type for `Sum`, or `None` if not summable.
fn sum_kind_of(ty: &CqlType) -> Option<SumKind> {
    match unwrap_frozen(ty) {
        CqlType::TinyInt
        | CqlType::SmallInt
        | CqlType::Int
        | CqlType::BigInt
        | CqlType::Counter => Some(SumKind::Integer),
        CqlType::Float | CqlType::Double => Some(SumKind::Float),
        _ => None,
    }
}

impl AggPlan {
    /// Validate `agg` against `schema`, resolving every column's type and
    /// rejecting bad specs (unknown column, `Sum`/`Min`/`Max` without a column,
    /// `Sum` on a non-numeric column).
    pub fn build(agg: &Aggregation, schema: &TableSchema) -> Result<Self, AggError> {
        let group_by = agg
            .group_by
            .iter()
            .map(|name| Ok((name.clone(), column_type(schema, name)?)))
            .collect::<Result<Vec<_>, AggError>>()?;

        let aggregates = agg
            .aggregates
            .iter()
            .map(|spec| Self::plan_one(spec, schema))
            .collect::<Result<Vec<_>, AggError>>()?;

        Ok(Self {
            group_by,
            aggregates,
        })
    }

    fn plan_one(spec: &AggregateSpec, schema: &TableSchema) -> Result<PlannedAggregate, AggError> {
        // count(*) is the only function that may omit a column.
        let (source_type, sum_kind) = match (&spec.func, &spec.column) {
            (AggFunc::Count, None) => (None, None),
            (_, None) => {
                return Err(AggError::MissingColumn {
                    output: spec.output.clone(),
                    func: spec.func,
                });
            }
            (func, Some(column)) => {
                let ty = column_type(schema, column)?;
                let kind = if matches!(func, AggFunc::Sum) {
                    let kind = sum_kind_of(&ty).ok_or_else(|| AggError::SumNotNumeric {
                        column: column.clone(),
                        cql_type: ty.clone(),
                    })?;
                    Some(kind)
                } else {
                    None
                };
                // Float/double min/max is not pushed (NaN ordering, see #896).
                if matches!(func, AggFunc::Min | AggFunc::Max)
                    && matches!(unwrap_frozen(&ty), CqlType::Float | CqlType::Double)
                {
                    return Err(AggError::MinMaxFloatUnsupported {
                        column: column.clone(),
                    });
                }
                (Some(ty), kind)
            }
        };

        Ok(PlannedAggregate {
            func: spec.func,
            column: spec.column.clone(),
            source_type,
            sum_kind,
            output: spec.output.clone(),
        })
    }

    /// Build the partial-output [`ColumnInfo`] list: group-by columns (reusing
    /// the table's mapped types so metadata like the UUID extension survives)
    /// followed by the aggregate output columns with their contract Arrow types.
    pub fn partial_columns(&self, schema: &TableSchema) -> Result<Vec<ColumnInfo>, AggError> {
        // schema_columns errors only on an unparseable DDL type; we already
        // validated every referenced column, so map its error generically.
        let table_columns =
            schema_columns(schema).map_err(|_| AggError::UnknownColumn(schema.table.clone()))?;

        let mut columns = Vec::with_capacity(self.group_by.len() + self.aggregates.len());

        for (position, (name, _)) in self.group_by.iter().enumerate() {
            let base = table_columns
                .iter()
                .find(|c| &c.name == name)
                .ok_or_else(|| AggError::UnknownColumn(name.clone()))?;
            let mut col = base.clone();
            col.position = position;
            columns.push(col);
        }

        for planned in &self.aggregates {
            let position = columns.len();
            let cql_type = self.output_cql_type(planned);
            columns.push(ColumnInfo {
                name: planned.output.clone(),
                data_type: flat_for(&cql_type),
                // Count is never null; sum/min/max may be null on an empty group.
                nullable: !matches!(planned.func, AggFunc::Count),
                position,
                table_name: Some(schema.table.clone()),
                cql_type: Some(cql_type),
            });
        }

        Ok(columns)
    }

    /// The Arrow-mapped `CqlType` of one aggregate's output column.
    fn output_cql_type(&self, planned: &PlannedAggregate) -> CqlType {
        match planned.func {
            // Count → Int64.
            AggFunc::Count => CqlType::BigInt,
            // Sum → Int64 (integer family) or Float64 (float family).
            AggFunc::Sum => match planned.sum_kind {
                Some(SumKind::Float) => CqlType::Double,
                // Integer family (or, defensively, missing kind) → Int64.
                _ => CqlType::BigInt,
            },
            // Min/Max keep the source column's type.
            AggFunc::Min | AggFunc::Max => planned.source_type.clone().unwrap_or(CqlType::BigInt),
        }
    }

    /// Start a fresh streaming aggregation state. For a global (empty `group_by`)
    /// aggregation the single group is created up-front so it is always emitted,
    /// even over zero input rows.
    pub fn new_state(&self) -> AggState {
        let mut groups: Vec<GroupState> = Vec::new();
        if self.group_by.is_empty() {
            groups.push(self.new_group_state(Vec::new()));
        }
        AggState {
            groups,
            index: HashMap::new(),
        }
    }

    /// Fold one surviving row into `state`. Only per-group accumulator state is
    /// retained — memory scales with the number of groups, NOT the input row
    /// count — so the producer can stream rows straight off the merge without
    /// buffering them all (issue #841 review).
    pub fn accumulate_row(&self, state: &mut AggState, row: &QueryRow) -> Result<(), AggError> {
        if self.group_by.is_empty() {
            // Global group is always at index 0 (created in `new_state`).
            return state.groups[0].accumulate(self, row);
        }
        let key: Vec<GroupKey> = self
            .group_by
            .iter()
            .map(|(name, _)| GroupKey::from_value(row.values.get(name)))
            .collect();
        // Split the index lookup from the group insert so neither borrows the
        // other (an `entry().or_insert_with` closure would double-borrow state).
        let pos = match state.index.get(&key) {
            Some(&pos) => pos,
            None => {
                // Carry the ORIGINAL row values as the group's output key — never
                // reconstruct from the hash bytes, which lose non-finite floats
                // (NaN/±Inf serialize to JSON null and would emit as NULL).
                let key_values: Vec<Option<Value>> = self
                    .group_by
                    .iter()
                    .map(|(name, _)| match row.values.get(name) {
                        None | Some(Value::Null) => None,
                        Some(v) => Some(v.clone()),
                    })
                    .collect();
                state.groups.push(self.new_group_state(key_values));
                let pos = state.groups.len() - 1;
                state.index.insert(key, pos);
                pos
            }
        };
        state.groups[pos].accumulate(self, row)
    }

    /// Finish a streaming aggregation, materializing one partial [`QueryRow`] per
    /// group (group-by key columns plus each aggregate's output).
    pub fn finish(&self, state: AggState) -> Vec<QueryRow> {
        state
            .groups
            .into_iter()
            .map(|g| g.into_query_row(self))
            .collect()
    }

    /// Run the aggregation over an in-memory slice of `rows` (convenience for
    /// tests). Production streams via [`Self::new_state`]/[`Self::accumulate_row`]/
    /// [`Self::finish`] instead, so the full row set is never buffered.
    ///
    /// Empty `group_by` always yields exactly one row (the global group), even
    /// when `rows` is empty.
    pub fn aggregate(&self, rows: &[QueryRow]) -> Result<Vec<QueryRow>, AggError> {
        let mut state = self.new_state();
        for row in rows {
            self.accumulate_row(&mut state, row)?;
        }
        Ok(self.finish(state))
    }

    fn new_group_state(&self, key_values: Vec<Option<Value>>) -> GroupState {
        GroupState {
            key_values,
            accs: self.aggregates.iter().map(Accumulator::new).collect(),
        }
    }
}

/// Streaming aggregation state: one [`GroupState`] per distinct group key, plus
/// an index for O(1) group lookup. Built by [`AggPlan::new_state`], fed one row
/// at a time via [`AggPlan::accumulate_row`], and drained by [`AggPlan::finish`].
/// Memory scales with the number of groups, not the input row count.
pub struct AggState {
    groups: Vec<GroupState>,
    index: HashMap<Vec<GroupKey>, usize>,
}

/// Map a `CqlType` to the flat `DataType` placeholder carried by `ColumnInfo`.
/// The Arrow converter always prefers `cql_type`, so this only needs to be a
/// structurally-consistent fallback.
fn flat_for(cql: &CqlType) -> DataType {
    match unwrap_frozen(cql) {
        CqlType::Boolean => DataType::Boolean,
        CqlType::TinyInt => DataType::TinyInt,
        CqlType::SmallInt => DataType::SmallInt,
        CqlType::Int => DataType::Integer,
        CqlType::BigInt | CqlType::Counter => DataType::BigInt,
        CqlType::Float => DataType::Float32,
        CqlType::Double => DataType::Float,
        CqlType::Text | CqlType::Ascii | CqlType::Varchar => DataType::Text,
        CqlType::Blob => DataType::Blob,
        CqlType::Timestamp => DataType::Timestamp,
        CqlType::Uuid | CqlType::TimeUuid => DataType::Uuid,
        _ => DataType::Text,
    }
}

/// A hashable, equatable group-key cell. NULL/absent group keys collapse to one
/// SQL-style group (`GroupKey::Null`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GroupKey {
    /// SQL NULL (an absent or `Value::Null` group cell).
    Null,
    /// A non-null group cell, keyed by its serialized form so any `Value`
    /// variant (text, int, uuid bytes, …) groups correctly without needing
    /// `Value: Ord/Hash`.
    NonNull(Vec<u8>),
}

impl GroupKey {
    /// Build a stable, hashable identity for a group cell. Used ONLY for grouping/
    /// dedup — the emitted key value comes from the original row `Value`, never
    /// from this serialized form (see [`AggPlan::accumulate_row`]).
    fn from_value(value: Option<&Value>) -> Self {
        match value {
            None | Some(Value::Null) => GroupKey::Null,
            Some(v) => GroupKey::NonNull(serde_json::to_vec(v).unwrap_or_default()),
        }
    }
}

/// One group's running aggregate state during a single pass.
struct GroupState {
    /// The group key per group-by column (`None` = SQL NULL).
    key_values: Vec<Option<Value>>,
    /// One accumulator per aggregate, parallel to [`AggPlan::aggregates`].
    accs: Vec<Accumulator>,
}

impl GroupState {
    fn accumulate(&mut self, plan: &AggPlan, row: &QueryRow) -> Result<(), AggError> {
        for (acc, planned) in self.accs.iter_mut().zip(plan.aggregates.iter()) {
            acc.update(planned, row)?;
        }
        Ok(())
    }

    fn into_query_row(self, plan: &AggPlan) -> QueryRow {
        let mut values: HashMap<String, Value> = HashMap::new();

        // Group-by key columns: omit a NULL key so the column reads as Arrow null.
        for ((name, _), value) in plan.group_by.iter().zip(self.key_values.iter()) {
            if let Some(v) = value {
                values.insert(name.clone(), v.clone());
            }
        }

        // Aggregate output columns: omit a None result so it reads as Arrow null.
        for (acc, planned) in self.accs.into_iter().zip(plan.aggregates.iter()) {
            if let Some(v) = acc.finalize(planned) {
                values.insert(planned.output.clone(), v);
            }
        }

        QueryRow {
            values,
            key: RowKey(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        }
    }
}

/// Per-aggregate running state.
enum Accumulator {
    /// `count(*)` or `count(col)` — a running non-null count.
    Count(i64),
    /// `sum` over an integer family — `None` until a first non-null input.
    SumInt(Option<i64>),
    /// `sum` over a float family — `None` until a first non-null input.
    SumFloat(Option<f64>),
    /// `min`/`max` over the source type — holds the current extreme `Value`.
    Extreme(Option<Value>),
}

impl Accumulator {
    fn new(planned: &PlannedAggregate) -> Self {
        match planned.func {
            AggFunc::Count => Accumulator::Count(0),
            AggFunc::Sum => match planned.sum_kind {
                Some(SumKind::Float) => Accumulator::SumFloat(None),
                _ => Accumulator::SumInt(None),
            },
            AggFunc::Min | AggFunc::Max => Accumulator::Extreme(None),
        }
    }

    fn update(&mut self, planned: &PlannedAggregate, row: &QueryRow) -> Result<(), AggError> {
        match self {
            Accumulator::Count(n) => {
                match &planned.column {
                    // count(*) counts every surviving row.
                    None => *n += 1,
                    // count(col) counts non-null values of col.
                    Some(col) => {
                        if !is_null(row.values.get(col)) {
                            *n += 1;
                        }
                    }
                }
            }
            Accumulator::SumInt(acc) => {
                if let Some(value) = non_null(&planned.column, row) {
                    if let Some(n) = as_i64(value) {
                        // Checked, not wrapping: a sum exceeding i64 must error to
                        // match Trino's non-pushed bigint sum (no silent wrap).
                        let next = acc.unwrap_or(0).checked_add(n).ok_or_else(|| {
                            AggError::SumOverflow {
                                column: planned
                                    .column
                                    .clone()
                                    .unwrap_or_else(|| planned.output.clone()),
                            }
                        })?;
                        *acc = Some(next);
                    }
                }
            }
            Accumulator::SumFloat(acc) => {
                if let Some(value) = non_null(&planned.column, row) {
                    if let Some(f) = as_f64(value) {
                        *acc = Some(acc.unwrap_or(0.0) + f);
                    }
                }
            }
            Accumulator::Extreme(current) => {
                if let Some(value) = non_null(&planned.column, row) {
                    let take = match current {
                        None => true,
                        Some(existing) => {
                            match compare_values(value, existing) {
                                Some(std::cmp::Ordering::Less) => {
                                    matches!(planned.func, AggFunc::Min)
                                }
                                Some(std::cmp::Ordering::Greater) => {
                                    matches!(planned.func, AggFunc::Max)
                                }
                                // Equal or incomparable: keep the existing value.
                                _ => false,
                            }
                        }
                    };
                    if take {
                        *current = Some(value.clone());
                    }
                }
            }
        }
        Ok(())
    }

    fn finalize(self, _planned: &PlannedAggregate) -> Option<Value> {
        match self {
            Accumulator::Count(n) => Some(Value::BigInt(n)),
            Accumulator::SumInt(acc) => acc.map(Value::BigInt),
            Accumulator::SumFloat(acc) => acc.map(Value::Float),
            Accumulator::Extreme(v) => v,
        }
    }
}

/// The non-null value of `column` in `row`, or `None` (absent/`Null`/no column).
fn non_null<'a>(column: &Option<String>, row: &'a QueryRow) -> Option<&'a Value> {
    let col = column.as_ref()?;
    match row.values.get(col) {
        None | Some(Value::Null) => None,
        Some(v) => Some(v),
    }
}

/// True when a looked-up column value is absent or SQL NULL.
fn is_null(value: Option<&Value>) -> bool {
    matches!(value, None | Some(Value::Null))
}

/// Widen any integer-family `Value` to `i64` for summation.
fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::TinyInt(v) => Some(*v as i64),
        Value::SmallInt(v) => Some(*v as i64),
        Value::Integer(v) => Some(*v as i64),
        Value::BigInt(v) | Value::Counter(v) => Some(*v),
        _ => None,
    }
}

/// Widen any float-family `Value` to `f64` for summation.
fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Float(v) => Some(*v),
        Value::Float32(v) => Some(*v as f64),
        _ => None,
    }
}

/// Total order over the `Value` variants that Min/Max can see (the source
/// column's type). Returns `None` for variants that are not mutually
/// comparable, in which case the accumulator keeps its current extreme.
///
/// Floats compare via `partial_cmp` falling back to `Equal`, so a `NaN` never
/// displaces the running extreme (matches "keep current on incomparable").
fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Boolean(x), Value::Boolean(y)) => Some(x.cmp(y)),
        (Value::TinyInt(x), Value::TinyInt(y)) => Some(x.cmp(y)),
        (Value::SmallInt(x), Value::SmallInt(y)) => Some(x.cmp(y)),
        (Value::Integer(x), Value::Integer(y)) => Some(x.cmp(y)),
        (Value::BigInt(x), Value::BigInt(y))
        | (Value::Counter(x), Value::Counter(y))
        | (Value::Timestamp(x), Value::Timestamp(y))
        | (Value::Time(x), Value::Time(y)) => Some(x.cmp(y)),
        (Value::Date(x), Value::Date(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => Some(x.partial_cmp(y).unwrap_or(Ordering::Equal)),
        (Value::Float32(x), Value::Float32(y)) => Some(x.partial_cmp(y).unwrap_or(Ordering::Equal)),
        (Value::Text(x), Value::Text(y)) => Some(x.cmp(y)),
        (Value::Blob(x), Value::Blob(y)) => Some(x.cmp(y)),
        (Value::Uuid(x), Value::Uuid(y)) => Some(x.cmp(y)),
        // Mixed integer widths (defensive — Min/Max source type is fixed).
        (lhs, rhs) => match (as_i64(lhs), as_i64(rhs)) {
            (Some(x), Some(y)) => Some(x.cmp(&y)),
            _ => match (as_f64(lhs), as_f64(rhs)) {
                (Some(x), Some(y)) => Some(x.partial_cmp(&y).unwrap_or(Ordering::Equal)),
                _ => None,
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cqlite_core::schema::{Column, KeyColumn};
    use std::collections::HashMap;

    /// Schema: pk id(int), regular v(bigint).
    fn bigint_schema() -> TableSchema {
        TableSchema {
            keyspace: "ks".into(),
            table: "t".into(),
            partition_keys: vec![KeyColumn {
                name: "id".into(),
                data_type: "int".into(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".into(),
                    data_type: "int".into(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "v".into(),
                    data_type: "bigint".into(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
        }
    }

    fn row_bigint(v: i64) -> QueryRow {
        let mut values = HashMap::new();
        values.insert("v".to_string(), Value::BigInt(v));
        QueryRow {
            values,
            key: RowKey(Vec::new()),
            metadata: Default::default(),
            cell_metadata: None,
        }
    }

    fn sum_v_plan() -> AggPlan {
        let agg = Aggregation {
            group_by: vec![],
            aggregates: vec![AggregateSpec {
                func: AggFunc::Sum,
                column: Some("v".into()),
                output: "agg0".into(),
            }],
        };
        AggPlan::build(&agg, &bigint_schema()).expect("plan")
    }

    #[test]
    fn integer_sum_overflow_errors_not_wraps() {
        let plan = sum_v_plan();
        let rows = vec![row_bigint(i64::MAX), row_bigint(1)];
        let err = plan
            .aggregate(&rows)
            .expect_err("sum past i64::MAX must error");
        assert!(matches!(err, AggError::SumOverflow { column } if column == "v"));
    }

    #[test]
    fn float_min_max_is_rejected_at_build() {
        // The schema's `v` is bigint; add a double column to test float min/max.
        let mut schema = bigint_schema();
        schema.columns.push(Column {
            name: "d".into(),
            data_type: "double".into(),
            nullable: true,
            default: None,
            is_static: false,
        });
        for func in [AggFunc::Min, AggFunc::Max] {
            let agg = Aggregation {
                group_by: vec![],
                aggregates: vec![AggregateSpec {
                    func,
                    column: Some("d".into()),
                    output: "agg0".into(),
                }],
            };
            let err = AggPlan::build(&agg, &schema).expect_err("float min/max must be rejected");
            assert!(matches!(err, AggError::MinMaxFloatUnsupported { column } if column == "d"));
        }
    }

    #[test]
    fn integer_sum_without_overflow_succeeds() {
        let plan = sum_v_plan();
        let rows = vec![row_bigint(10), row_bigint(32)];
        let out = plan.aggregate(&rows).expect("no overflow");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].values.get("agg0"), Some(&Value::BigInt(42)));
    }
}
