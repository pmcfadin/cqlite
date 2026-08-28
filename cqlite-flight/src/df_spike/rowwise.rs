//! Row-at-a-time evaluation over an Arrow batch — the ROW-ENGINE arm of the
//! spike benchmark (issue #2605).
//!
//! # What this arm represents, and the caveat that matters
//!
//! The comparison the throughput program asks for is "row-engine execution vs
//! DataFusion execution over the SAME already-produced batches". So this arm
//! evaluates the scenario the way a row engine does — one row at a time, one
//! scalar comparison at a time — over exactly the batches the DataFusion arm
//! consumes.
//!
//! **This is a CONSERVATIVE row-side baseline: it understates the real row
//! engine.** CQLite's production row engine evaluates predicates against a
//! `QueryRow`, i.e. a `HashMap<String, Value>` — so per cell it additionally
//! pays a string hash lookup and a `Value` enum construction that this arm does
//! not. Downcasting each column ONCE per batch and then indexing it is strictly
//! cheaper. Any vectorized advantage this harness reports is therefore a LOWER
//! BOUND on the advantage over the production row path, never an inflated one —
//! which is the direction an honest measurement should err in.
//!
//! Only the scalar types the benchmark's filter column can have are handled; an
//! unsupported type is an ERROR, never a silently-false comparison (a predicate
//! that quietly matches nothing would make this arm look arbitrarily fast).

use arrow::array::{Array, BooleanArray, Float32Array, Float64Array};
use arrow::array::{Int16Array, Int32Array, Int64Array, Int8Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

/// A scalar comparison operator for the row-wise arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOp {
    /// `=`
    Eq,
    /// `!=`
    NotEq,
    /// `<`
    Lt,
    /// `<=`
    LtEq,
    /// `>`
    Gt,
    /// `>=`
    GtEq,
}

impl RowOp {
    /// Parse a CLI spelling.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "eq" | "=" => Some(Self::Eq),
            "ne" | "neq" | "!=" | "<>" => Some(Self::NotEq),
            "lt" | "<" => Some(Self::Lt),
            "lte" | "le" | "<=" => Some(Self::LtEq),
            "gt" | ">" => Some(Self::Gt),
            "gte" | "ge" | ">=" => Some(Self::GtEq),
            _ => None,
        }
    }

    /// SQL spelling, for the DataFusion arm's query text.
    pub fn sql(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "<>",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
        }
    }

    /// Apply to an `Ordering`-shaped comparison result.
    fn keeps(self, ordering: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::{Equal, Greater, Less};
        matches!(
            (self, ordering),
            (Self::Eq, Equal)
                | (Self::NotEq, Less | Greater)
                | (Self::Lt, Less)
                | (Self::LtEq, Less | Equal)
                | (Self::Gt, Greater)
                | (Self::GtEq, Greater | Equal)
        )
    }
}

/// The literal the filter compares against, in the two shapes the benchmark
/// needs. Parsed from the CLI, resolved against the column's Arrow type at
/// evaluation time.
#[derive(Debug, Clone)]
pub enum RowLiteral {
    /// An integral operand.
    Int(i64),
    /// A floating-point operand.
    Float(f64),
    /// A text operand.
    Text(String),
    /// A boolean operand.
    Bool(bool),
}

impl RowLiteral {
    /// Parse a CLI operand: `true`/`false`, then integer, then float, then text.
    pub fn parse(raw: &str) -> Self {
        if raw == "true" {
            return Self::Bool(true);
        }
        if raw == "false" {
            return Self::Bool(false);
        }
        if let Ok(v) = raw.parse::<i64>() {
            return Self::Int(v);
        }
        if let Ok(v) = raw.parse::<f64>() {
            if v.is_finite() {
                return Self::Float(v);
            }
        }
        Self::Text(raw.to_string())
    }

    /// SQL literal text for the DataFusion arm.
    pub fn sql(&self) -> String {
        match self {
            Self::Int(v) => v.to_string(),
            Self::Float(v) => v.to_string(),
            Self::Bool(v) => v.to_string(),
            // Single quotes doubled, per SQL string-literal escaping.
            Self::Text(v) => format!("'{}'", v.replace('\'', "''")),
        }
    }

    /// JSON form for the pushdown path (matching `crate::filter`'s operands).
    pub fn json(&self) -> serde_json::Value {
        match self {
            Self::Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
            Self::Float(v) => serde_json::Number::from_f64(*v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Self::Bool(v) => serde_json::Value::Bool(*v),
            Self::Text(v) => serde_json::Value::String(v.clone()),
        }
    }
}

/// Errors from the row-wise arm.
#[derive(Debug, thiserror::Error)]
pub enum RowWiseError {
    /// The filter column is not in the batch.
    #[error("column '{0}' is not present in the produced batch")]
    MissingColumn(String),
    /// The column's Arrow type is not one this arm compares.
    #[error("row-wise arm cannot compare column '{column}' of Arrow type {data_type}")]
    UnsupportedType {
        /// Column name.
        column: String,
        /// Its Arrow type.
        data_type: String,
    },
    /// The literal's shape does not match the column's type.
    #[error("operand {operand} does not match column '{column}' of Arrow type {data_type}")]
    OperandMismatch {
        /// Column name.
        column: String,
        /// Its Arrow type.
        data_type: String,
        /// The operand as given.
        operand: String,
    },
}

/// Count rows one at a time — the row-engine analogue of `count(*)`.
///
/// Deliberately a per-row loop rather than `num_rows()`: the arm under test is
/// "the row engine walks every row", and reading the length would measure
/// nothing at all.
pub(crate) fn count_rows_rowwise(batch: &RecordBatch) -> u64 {
    let mut counted: u64 = 0;
    for _ in 0..batch.num_rows() {
        counted += 1;
    }
    counted
}

/// Count rows matching `column op literal`, one row at a time.
pub(crate) fn count_matching_rowwise(
    batch: &RecordBatch,
    column: &str,
    op: RowOp,
    literal: &RowLiteral,
) -> Result<u64, RowWiseError> {
    let index = batch
        .schema()
        .index_of(column)
        .map_err(|_| RowWiseError::MissingColumn(column.to_string()))?;
    let array = batch.column(index);
    let data_type = array.data_type().clone();

    macro_rules! numeric_int {
        ($ty:ty, $lit:expr) => {{
            let typed = array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                RowWiseError::UnsupportedType {
                    column: column.to_string(),
                    data_type: data_type.to_string(),
                }
            })?;
            let mut matched: u64 = 0;
            for i in 0..typed.len() {
                // A NULL cell yields SQL UNKNOWN, which rejects the row — the
                // same answer `FilterExpr::keeps` gives.
                if typed.is_null(i) {
                    continue;
                }
                let value = i64::from(typed.value(i));
                if op.keeps(value.cmp(&$lit)) {
                    matched += 1;
                }
            }
            Ok(matched)
        }};
    }

    match (&data_type, literal) {
        (DataType::Int8, RowLiteral::Int(lit)) => numeric_int!(Int8Array, *lit),
        (DataType::Int16, RowLiteral::Int(lit)) => numeric_int!(Int16Array, *lit),
        (DataType::Int32, RowLiteral::Int(lit)) => numeric_int!(Int32Array, *lit),
        (DataType::Int64, RowLiteral::Int(lit)) => {
            let typed = downcast::<Int64Array>(array, column, &data_type)?;
            Ok(count_scalar(
                typed.len(),
                |i| (!typed.is_null(i)).then(|| typed.value(i).cmp(lit)),
                op,
            ))
        }
        (DataType::Float32, RowLiteral::Float(lit)) => {
            let typed = downcast::<Float32Array>(array, column, &data_type)?;
            Ok(count_scalar(
                typed.len(),
                |i| (!typed.is_null(i)).then(|| compare_f64(f64::from(typed.value(i)), *lit)),
                op,
            ))
        }
        (DataType::Float64, RowLiteral::Float(lit)) => {
            let typed = downcast::<Float64Array>(array, column, &data_type)?;
            Ok(count_scalar(
                typed.len(),
                |i| (!typed.is_null(i)).then(|| compare_f64(typed.value(i), *lit)),
                op,
            ))
        }
        (DataType::Utf8, RowLiteral::Text(lit)) => {
            let typed = downcast::<StringArray>(array, column, &data_type)?;
            Ok(count_scalar(
                typed.len(),
                |i| (!typed.is_null(i)).then(|| typed.value(i).cmp(lit.as_str())),
                op,
            ))
        }
        (DataType::Boolean, RowLiteral::Bool(lit)) => {
            let typed = downcast::<BooleanArray>(array, column, &data_type)?;
            Ok(count_scalar(
                typed.len(),
                |i| (!typed.is_null(i)).then(|| typed.value(i).cmp(lit)),
                op,
            ))
        }
        (
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Boolean,
            other,
        ) => Err(RowWiseError::OperandMismatch {
            column: column.to_string(),
            data_type: data_type.to_string(),
            operand: format!("{other:?}"),
        }),
        _ => Err(RowWiseError::UnsupportedType {
            column: column.to_string(),
            data_type: data_type.to_string(),
        }),
    }
}

/// Downcast helper reporting an unsupported type rather than panicking.
fn downcast<'a, T: 'static>(
    array: &'a dyn Array,
    column: &str,
    data_type: &DataType,
) -> Result<&'a T, RowWiseError> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| RowWiseError::UnsupportedType {
            column: column.to_string(),
            data_type: data_type.to_string(),
        })
}

/// The shared per-row loop: `ordering(i)` is `None` for a NULL cell (SQL
/// UNKNOWN, row rejected) and `Some(ordering)` otherwise.
fn count_scalar(
    len: usize,
    ordering: impl Fn(usize) -> Option<std::cmp::Ordering>,
    op: RowOp,
) -> u64 {
    let mut matched: u64 = 0;
    for i in 0..len {
        if let Some(o) = ordering(i) {
            if op.keeps(o) {
                matched += 1;
            }
        }
    }
    matched
}

/// Compare two `f64`s with Cassandra/Java `Double.compare` semantics (NaN last,
/// `-0.0 < +0.0`) rather than `total_cmp` or a partial compare that would drop
/// NaN silently.
fn compare_f64(a: f64, b: f64) -> std::cmp::Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => std::cmp::Ordering::Equal,
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        (false, false) => a.total_cmp(&b),
    }
}
