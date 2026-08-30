//! EXPECTED Arrow type for a declared CQL type — the type half of the parity
//! oracle (issue #1490).
//!
//! # Why this exists
//!
//! Value canonicalization is deliberately width-blind: `Int8`, `Int16`, `Int32`
//! and `Int64` all project to one `CanonicalValue::Int`, and `Float32`/`Float64`
//! to one `CanonicalValue::Float`. That is what makes the per-cell comparison
//! possible at all — but it means a WRONG CQL→Arrow mapping (a `tinyint`
//! exported as `Int64`, a `smallint` as `Int32`, a UDT flattened to `Utf8`)
//! round-trips its values unchanged and passes. A harness blind to that class is
//! blind to the very defect family it was built for: #3556, the export bug this
//! lane found, IS a type-mapping bug (a UDT written as a `Utf8` rendering).
//!
//! So every Parquet field's Arrow type is validated BEFORE any value is
//! compared, against an expectation derived from the case's INDEPENDENTLY
//! DECLARED CQL type (copied from the committed `test-data/schemas/*.cql`).
//! Reading the expectation back out of CQLite's own mapping code would be
//! circular — a CQLite `file:line` is evidence of what CQLite does, never of
//! what is correct (#3041).
//!
//! # Where each expectation comes from
//!
//! Fixed-width CQL scalars have exactly one faithful Arrow type, and the width
//! is the point: a downstream Arrow/DuckDB/Trino consumer sees the declared
//! column width, so widening `tinyint` to `Int64` is a real interoperability
//! defect even though the values compare equal.
//!
//! | declared CQL              | expected Arrow                        | authority |
//! |---------------------------|---------------------------------------|-----------|
//! | `boolean`                 | `Boolean`                             | 1-bit domain |
//! | `tinyint`                 | `Int8`                                | Cassandra `ByteType`, 8-bit signed |
//! | `smallint`                | `Int16`                               | `ShortType`, 16-bit signed |
//! | `int`                     | `Int32`                               | `Int32Type` |
//! | `bigint` / `counter`      | `Int64`                               | `LongType` / `CounterColumnType` |
//! | `float`                   | `Float32`                             | IEEE binary32 |
//! | `double`                  | `Float64`                             | IEEE binary64 |
//! | `text`/`varchar`/`ascii`  | `Utf8` (or `LargeUtf8`)               | UTF-8 string domain |
//! | `blob`                    | `Binary` (or `LargeBinary`)           | opaque bytes |
//! | `uuid` / `timeuuid`       | `FixedSizeBinary(16)`                 | 128-bit UUID; Arrow's `arrow.uuid` extension storage |
//! | `timestamp`               | `Timestamp(Millisecond, UTC)`         | Cassandra timestamps are epoch MILLIS, UTC |
//! | `date`                    | `Date32`                              | days since epoch |
//! | `time`                    | `Time64(Nanosecond)`                  | nanos since midnight |
//! | `decimal`                 | `Decimal128(p ≤ 38, s ≥ 0)`           | see below |
//! | `varint`                  | `Decimal128(p ≤ 38, 0)`               | integer domain, arbitrary precision |
//! | `inet`                    | `Utf8`                                | see below |
//! | `duration`                | `Interval(MonthDayNano)` or `Utf8`    | see below |
//! | `list<E>` / `set<E>`      | `List<expected(E)>`                   | Arrow has no Set type |
//! | `map<K,V>`                | `Map<expected(K), expected(V)>`       | Arrow Map |
//! | `tuple<A,B,…>`            | `Struct` of arity n, positional       | Arrow Struct |
//! | UDT                       | `Struct` (field types not declared)   | Arrow Struct |
//!
//! Three mappings are genuinely ambiguous, so the expectation is anchored
//! explicitly rather than left to taste:
//!
//!   * **`decimal`.** CQL `decimal` is arbitrary precision AND arbitrary scale;
//!     Arrow has no such type, so any `Decimal128(p, s)` is a lossy encoding
//!     CHOICE, not a wrong type. The expectation therefore fixes the type FAMILY
//!     (128-bit decimal, `p ≤ 38`, `s ≥ 0`) and leaves the scale free — a scale
//!     too small to hold a value is a VALUE defect, which the per-cell
//!     comparison catches on its own.
//!   * **`varint`.** Arbitrary-precision INTEGER, so `Decimal128(_, 0)` is
//!     faithful while `Int64` would silently truncate; scale is pinned to 0.
//!   * **`duration`.** The faithful Arrow type is
//!     `Interval(MonthDayNano)` — months/days/nanos, exactly CQL's
//!     three-component model. parquet-format's INTERVAL logical type carries only
//!     months/days/MILLIS, so it cannot persist CQL nanos; a textual
//!     `Utf8` spelling is the accepted lossless alternative (the harness
//!     normalizes the two writers' spellings in `spelling.rs`). BOTH are
//!     accepted, and nothing else is.
//!   * **`inet`.** Arrow has no address type. `Utf8` carrying Cassandra's
//!     canonical textual form is expected, because that is what `sstabledump`
//!     writes and therefore what the oracle can be compared against; a switch to
//!     `Binary(4|16)` is a deliberate decision that SHOULD red this harness.
//!
//! # What is deliberately NOT asserted
//!
//! * **Nullability and field names.** Arrow's nested-child names (`item`,
//!   `entries`/`key`/`value`, `field_0`) are conventions a Parquet round-trip may
//!   respell, and CQL makes every non-key column nullable, so neither carries
//!   information about the CQL→Arrow mapping's correctness.
//! * **UDT field types.** A case declares a UDT by NAME only (the harness never
//!   needs its field types — sstabledump types the field values for it), so the
//!   expectation is "an Arrow `Struct`" and its fields are unconstrained. That is
//!   still enough to catch #3556's `Utf8` flattening.

#![allow(dead_code)]

use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};

use super::cql_type::{ColumnType, CqlTypeSpec};

/// An accepted Arrow type for one declared CQL type.
#[derive(Debug, Clone)]
pub enum ArrowShape {
    /// Exactly one of these Arrow types, compared by equality.
    OneOf(Vec<DataType>),
    /// `Decimal128(p ≤ 38, s)`; `scale = Some(0)` pins `varint`'s integer domain.
    Decimal128 { scale: Option<i8> },
    /// `Timestamp(Millisecond, <a UTC zone>)`.
    UtcMillisTimestamp,
    /// A list-like Arrow type whose element type matches.
    List(Box<ArrowShape>),
    /// An Arrow `Map` whose key and value types match.
    Map(Box<ArrowShape>, Box<ArrowShape>),
    /// An Arrow `Struct` of exactly this arity, matched positionally (`tuple`).
    Tuple(Vec<ArrowShape>),
    /// An Arrow `Struct` with unconstrained fields (a UDT, named for diagnostics).
    UdtStruct(String),
}

/// Derive the expected Arrow shape from a parsed declared CQL type.
pub fn expected_shape(spec: &CqlTypeSpec) -> Result<ArrowShape, String> {
    Ok(match spec {
        CqlTypeSpec::Scalar(name) => scalar_shape(name)?,
        CqlTypeSpec::Seq { elem, .. } => ArrowShape::List(Box::new(expected_shape(elem)?)),
        CqlTypeSpec::Map { key, value } => ArrowShape::Map(
            Box::new(expected_shape(key)?),
            Box::new(expected_shape(value)?),
        ),
        CqlTypeSpec::Tuple(specs) => ArrowShape::Tuple(
            specs
                .iter()
                .map(expected_shape)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        CqlTypeSpec::Udt(name) => ArrowShape::UdtStruct(name.clone()),
    })
}

fn scalar_shape(name: &str) -> Result<ArrowShape, String> {
    let one = |t: DataType| ArrowShape::OneOf(vec![t]);
    Ok(match name {
        "boolean" => one(DataType::Boolean),
        "tinyint" => one(DataType::Int8),
        "smallint" => one(DataType::Int16),
        "int" => one(DataType::Int32),
        "bigint" | "counter" => one(DataType::Int64),
        "float" => one(DataType::Float32),
        "double" => one(DataType::Float64),
        "text" | "varchar" | "ascii" => {
            ArrowShape::OneOf(vec![DataType::Utf8, DataType::LargeUtf8])
        }
        "blob" => ArrowShape::OneOf(vec![DataType::Binary, DataType::LargeBinary]),
        "uuid" | "timeuuid" => one(DataType::FixedSizeBinary(16)),
        "timestamp" => ArrowShape::UtcMillisTimestamp,
        "date" => one(DataType::Date32),
        "time" => one(DataType::Time64(TimeUnit::Nanosecond)),
        "decimal" => ArrowShape::Decimal128 { scale: None },
        "varint" => ArrowShape::Decimal128 { scale: Some(0) },
        "inet" => one(DataType::Utf8),
        // See the module header: Interval(MonthDayNano) is the faithful Arrow
        // type, Utf8 the accepted lossless substitute Parquet forces.
        "duration" => ArrowShape::OneOf(vec![
            DataType::Interval(IntervalUnit::MonthDayNano),
            DataType::Utf8,
        ]),
        other => {
            return Err(format!(
                "no expected Arrow type is declared for CQL scalar '{other}' — add it to \
                 arrow_expect.rs (anchored on Cassandra/Arrow semantics) rather than \
                 letting the type go unchecked"
            ))
        }
    })
}

impl ArrowShape {
    /// Does `actual` satisfy this expectation? (Nullability and nested field
    /// NAMES are deliberately not part of the answer — see the module header.)
    pub fn accepts(&self, actual: &DataType) -> bool {
        match self {
            ArrowShape::OneOf(types) => types.iter().any(|t| t == actual),
            ArrowShape::Decimal128 { scale } => match actual {
                DataType::Decimal128(precision, s) => {
                    let scale_ok = match scale {
                        Some(want) => *s == *want,
                        None => *s >= 0,
                    };
                    *precision <= 38 && scale_ok
                }
                _ => false,
            },
            ArrowShape::UtcMillisTimestamp => match actual {
                DataType::Timestamp(TimeUnit::Millisecond, Some(tz)) => {
                    let tz = tz.as_ref();
                    tz.eq_ignore_ascii_case("UTC")
                        || tz.eq_ignore_ascii_case("Etc/UTC")
                        || tz == "+00:00"
                        || tz == "Z"
                }
                _ => false,
            },
            ArrowShape::List(elem) => match actual {
                DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => {
                    elem.accepts(f.data_type())
                }
                _ => false,
            },
            ArrowShape::Map(key, value) => match actual {
                DataType::Map(entries, _) => match entries.data_type() {
                    DataType::Struct(fields) if fields.len() == 2 => {
                        key.accepts(fields[0].data_type()) && value.accepts(fields[1].data_type())
                    }
                    _ => false,
                },
                _ => false,
            },
            ArrowShape::Tuple(specs) => match actual {
                DataType::Struct(fields) if fields.len() == specs.len() => specs
                    .iter()
                    .zip(fields.iter())
                    .all(|(s, f)| s.accepts(f.data_type())),
                _ => false,
            },
            ArrowShape::UdtStruct(_) => matches!(actual, DataType::Struct(_)),
        }
    }

    /// Compact rendering of the expectation, for the mismatch message.
    pub fn describe(&self) -> String {
        match self {
            ArrowShape::OneOf(types) => types
                .iter()
                .map(render_arrow)
                .collect::<Vec<_>>()
                .join(" | "),
            ArrowShape::Decimal128 { scale: None } => "decimal128(p<=38,s>=0)".to_string(),
            ArrowShape::Decimal128 { scale: Some(s) } => format!("decimal128(p<=38,{s})"),
            ArrowShape::UtcMillisTimestamp => "timestamp(ms,UTC)".to_string(),
            ArrowShape::List(elem) => format!("list<{}>", elem.describe()),
            ArrowShape::Map(k, v) => format!("map<{},{}>", k.describe(), v.describe()),
            ArrowShape::Tuple(specs) => format!(
                "struct<{}>",
                specs
                    .iter()
                    .map(|s| s.describe())
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            ArrowShape::UdtStruct(name) => format!("struct(udt '{name}')"),
        }
    }
}

/// Compact, stable rendering of an ACTUAL Arrow type.
///
/// `Debug` on `DataType` prints whole `Field` structs (names, nullability,
/// metadata), which buries the one thing the message is about. This renders the
/// type only, in the same vocabulary [`ArrowShape::describe`] uses, so expected
/// and actual are directly comparable in the failure text.
pub fn render_arrow(t: &DataType) -> String {
    match t {
        DataType::Null => "null".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float16 => "float16".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::LargeUtf8 => "large_utf8".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::LargeBinary => "large_binary".to_string(),
        DataType::FixedSizeBinary(n) => format!("fixed_size_binary({n})"),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Time32(u) => format!("time32({})", render_time_unit(u)),
        DataType::Time64(u) => format!("time64({})", render_time_unit(u)),
        DataType::Timestamp(u, tz) => match tz {
            Some(tz) => format!("timestamp({},{tz})", render_time_unit(u)),
            None => format!("timestamp({},no-tz)", render_time_unit(u)),
        },
        DataType::Duration(u) => format!("duration({})", render_time_unit(u)),
        DataType::Interval(IntervalUnit::MonthDayNano) => "interval(month_day_nano)".to_string(),
        DataType::Interval(u) => format!("interval({u:?})"),
        DataType::Decimal128(p, s) => format!("decimal128({p},{s})"),
        DataType::Decimal256(p, s) => format!("decimal256({p},{s})"),
        DataType::List(f) => format!("list<{}>", render_arrow(f.data_type())),
        DataType::LargeList(f) => format!("large_list<{}>", render_arrow(f.data_type())),
        DataType::FixedSizeList(f, n) => {
            format!("fixed_size_list<{},{n}>", render_arrow(f.data_type()))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => format!(
                "map<{},{}>",
                render_arrow(fields[0].data_type()),
                render_arrow(fields[1].data_type())
            ),
            other => format!("map<malformed-entries:{}>", render_arrow(other)),
        },
        DataType::Struct(fields) => format!(
            "struct<{}>",
            fields
                .iter()
                .map(|f: &std::sync::Arc<Field>| format!(
                    "{}:{}",
                    f.name(),
                    render_arrow(f.data_type())
                ))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => format!("{other:?}"),
    }
}

fn render_time_unit(u: &TimeUnit) -> &'static str {
    match u {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

/// Validate ONE Parquet field's Arrow type against its declared CQL type.
///
/// The message names the column, the declared CQL type, the expected Arrow type
/// and the actual one — the four facts needed to decide whether the export or
/// the expectation is wrong.
pub fn validate_field(case_id: &str, col: &ColumnType, actual: &DataType) -> Result<(), String> {
    let shape =
        expected_shape(&col.spec).map_err(|e| format!("{case_id}: column '{}': {e}", col.name))?;
    if shape.accepts(actual) {
        return Ok(());
    }
    Err(format!(
        "Arrow type mismatch for column '{}' declared '{}': expected {}, got {}",
        col.name,
        col.declared,
        shape.describe(),
        render_arrow(actual)
    ))
}
