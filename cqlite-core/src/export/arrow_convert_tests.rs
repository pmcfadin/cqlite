//! Tests — fail-closed Value→Arrow conversion (issue #1485).
//!
//! Loaded via `#[path]` from `arrow_convert.rs` so the production modules stay
//! under the campsite file-size threshold (epic #1116; issue #3096 Phase 0a).
//! `super::*` therefore resolves against `export::arrow_convert`, exactly as it
//! did when this module was inline.

use super::*;
// The fail-closed i32 offset/byte guards moved to the shared
// `arrow_convert_util` module in the epic #1116 split; the assertions below
// exercise them directly, unchanged.
use crate::export::arrow_convert_util::{
    checked_binary_offsets, checked_offset, checked_string_offsets, checked_value_bytes,
};
use crate::query::{ColumnInfo, QueryRow};
use crate::schema::CqlType;
use crate::types::{DataType, Value};
use crate::RowKey;
use arrow::array::{Array, Float32Array, Int32Array, StringArray};
use std::collections::HashMap;
use std::sync::Arc;

/// Build a `ColumnInfo` for a single test column.
fn col(name: &str, data_type: DataType, cql_type: Option<CqlType>) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type,
    }
}

/// Build a `QueryRow` from a single (column, value) pair.
fn row_one(name: &str, value: Value) -> QueryRow {
    let mut values: HashMap<Arc<str>, Value> = HashMap::new();
    values.insert(name.into(), value);
    QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

/// An empty row: the column is absent entirely.
fn row_absent() -> QueryRow {
    QueryRow {
        values: HashMap::new(),
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

fn is_invalid_value(res: Result<arrow::record_batch::RecordBatch, ArrowConvertError>) -> bool {
    matches!(res, Err(ArrowConvertError::InvalidValue(_)))
}

/// (1) Typed high-fidelity scalar builder (`build_date32_array`): a
/// type-mismatched value must FAIL CLOSED rather than silently become NULL.
#[test]
fn typed_scalar_type_mismatch_is_error() {
    let columns = vec![col("d", DataType::Timestamp, Some(CqlType::Date))];
    let rows = vec![row_one("d", Value::Text("not-a-date".into()))];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (2) Flat `data_type` builder path (`build_int32_array`, `cql_type = None`):
/// a type-mismatched value must FAIL CLOSED.
#[test]
fn flat_builder_type_mismatch_is_error() {
    let columns = vec![col("n", DataType::Integer, None)];
    let rows = vec![row_one("n", Value::Text("nope".into()))];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (3a) Collection path (`build_typed_value_array` List arm): a scalar where
/// a list is expected must FAIL CLOSED.
#[test]
fn collection_expected_list_got_scalar_is_error() {
    let columns = vec![col(
        "l",
        DataType::List,
        Some(CqlType::List(Box::new(CqlType::Int))),
    )];
    let rows = vec![row_one("l", Value::Integer(5))];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (3b) Collection element dispatch (Pattern A scalar arm reached via list
/// recursion): a mistyped element inside a well-formed list must FAIL CLOSED.
#[test]
fn collection_mistyped_element_is_error() {
    let columns = vec![col(
        "l",
        DataType::List,
        Some(CqlType::List(Box::new(CqlType::Int))),
    )];
    let rows = vec![row_one(
        "l",
        Value::List(vec![Value::Integer(1), Value::Text("bad".into())]),
    )];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (3c) Map path (`build_typed_value_array` Map arm): a scalar where a map is
/// expected must FAIL CLOSED.
#[test]
fn collection_expected_map_got_scalar_is_error() {
    let columns = vec![col(
        "m",
        DataType::Map,
        Some(CqlType::Map(
            Box::new(CqlType::Text),
            Box::new(CqlType::Int),
        )),
    )];
    let rows = vec![row_one("m", Value::Integer(7))];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (4) Regression guard: `Value::Null` and an ABSENT column must STILL map to
/// proper Arrow nulls — never an error.
#[test]
fn null_and_absent_still_build_ok() {
    let columns = vec![col("n", DataType::Integer, None)];
    let rows = vec![row_one("n", Value::Null), row_absent()];
    let batch = rows_to_record_batch(&columns, &rows).expect("null/absent must build");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(0).null_count(), 2);
}

/// (5) Happy path: a correctly-typed value converts cleanly.
#[test]
fn correctly_typed_value_builds_ok() {
    let columns = vec![col("n", DataType::Integer, None)];
    let rows = vec![row_one("n", Value::Integer(42))];
    let batch = rows_to_record_batch(&columns, &rows).expect("well-typed value must build");
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32Array");
    assert_eq!(arr.value(0), 42);
    assert_eq!(arr.null_count(), 0);
}

/// (5b) Fail-closed (issue #1487): a `decimal` with scale > `DECIMAL_FIXED_SCALE`
/// (here scale 12) must return an error rather than silently truncating toward
/// zero. On the pre-fix code path this scaled down and succeeded lossily.
#[test]
fn decimal_scale_above_fixed_is_error() {
    let columns = vec![col("d", DataType::Blob, Some(CqlType::Decimal))];
    // 123456789012 with scale 12 == 0.123456789012 — 12 fractional digits.
    let unscaled = num_bigint::BigInt::from(123_456_789_012i64).to_signed_bytes_be();
    let rows = vec![row_one(
        "d",
        Value::Decimal {
            scale: 12,
            unscaled,
        },
    )];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (5c) Happy path (issue #1487): an in-range `decimal` (scale <= 9) still
/// converts exactly as before.
#[test]
fn decimal_scale_within_fixed_builds_ok() {
    use arrow::array::Decimal128Array;
    let columns = vec![col("d", DataType::Blob, Some(CqlType::Decimal))];
    // 123456 with scale 3 == 123.456 — rescaled to scale 9 -> 123_456_000_000.
    let unscaled = num_bigint::BigInt::from(123_456i64).to_signed_bytes_be();
    let rows = vec![row_one("d", Value::Decimal { scale: 3, unscaled })];
    let batch = rows_to_record_batch(&columns, &rows).expect("in-range decimal must build");
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("Decimal128Array");
    assert_eq!(arr.value(0), 123_456_000_000i128);
    assert_eq!(arr.null_count(), 0);
}

/// (5d) Regression guard (issue #1487): a NULL / absent decimal stays NULL —
/// the fail-closed scale check must not disturb the null path.
#[test]
fn decimal_null_and_absent_still_null() {
    let columns = vec![col("d", DataType::Blob, Some(CqlType::Decimal))];
    let rows = vec![row_one("d", Value::Null), row_absent()];
    let batch = rows_to_record_batch(&columns, &rows).expect("null/absent decimal must build");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(0).null_count(), 2);
}

/// (6) Regression: a CQL `float` (32-bit) column whose value is carried as
/// the wider `Value::Float` (f64) by the decode path must convert (narrowed
/// to f32), NOT be rejected as a type mismatch. Real data (e.g. a `height`
/// float column) surfaces `Value::Float`; the old silent `_ => None`
/// dropped it to NULL. Covers both the typed and flat float32 arms.
#[test]
fn float32_column_accepts_wide_float_value() {
    // Flat path (cql_type = None -> build_float32_array).
    let flat = vec![col("h", DataType::Float32, None)];
    let rows = vec![row_one("h", Value::Float(1.84f32 as f64))];
    let batch = rows_to_record_batch(&flat, &rows).expect("wide float must narrow, not error");
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Float32Array");
    assert_eq!(arr.value(0), 1.84f32);
    assert_eq!(arr.null_count(), 0);

    // Typed high-fidelity path (CqlType::Float -> build_typed_value_array).
    let typed = vec![col("h", DataType::Float32, Some(CqlType::Float))];
    let rows = vec![row_one("h", Value::Float(1.84f32 as f64))];
    let batch =
        rows_to_record_batch(&typed, &rows).expect("wide float (typed) must narrow, not error");
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float32Array>()
        .expect("Float32Array");
    assert_eq!(arr.value(0), 1.84f32);
}

/// (7a) Tuple arm: a non-`Tuple` top-level value in a tuple column must FAIL
/// CLOSED, not silently become a struct row of null children.
#[test]
fn tuple_expected_tuple_got_scalar_is_error() {
    let columns = vec![col(
        "t",
        DataType::Text,
        Some(CqlType::Tuple(vec![CqlType::Int, CqlType::Text])),
    )];
    let rows = vec![row_one("t", Value::Text("not-a-tuple".into()))];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (7b) Tuple arm regression: `Value::Null` and an ABSENT tuple column must
/// STILL build (as struct nulls), never error.
#[test]
fn tuple_null_and_absent_still_build_ok() {
    let columns = vec![col(
        "t",
        DataType::Text,
        Some(CqlType::Tuple(vec![CqlType::Int, CqlType::Text])),
    )];
    let rows = vec![row_one("t", Value::Null), row_absent()];
    let batch = rows_to_record_batch(&columns, &rows).expect("null/absent tuple must build");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(0).null_count(), 2);
}

/// (8a) UDT arm: a non-`Udt` top-level value in a UDT column must FAIL
/// CLOSED, not silently become a struct row of null children.
#[test]
fn udt_expected_udt_got_scalar_is_error() {
    let columns = vec![col(
        "u",
        DataType::Text,
        Some(CqlType::Udt(
            "my_type".into(),
            vec![("a".into(), CqlType::Int), ("b".into(), CqlType::Text)],
        )),
    )];
    let rows = vec![row_one("u", Value::Integer(9))];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (8b) UDT arm regression: `Value::Null` and an ABSENT UDT column must STILL
/// build (as struct nulls), never error.
#[test]
fn udt_null_and_absent_still_build_ok() {
    let columns = vec![col(
        "u",
        DataType::Text,
        Some(CqlType::Udt(
            "my_type".into(),
            vec![("a".into(), CqlType::Int), ("b".into(), CqlType::Text)],
        )),
    )];
    let rows = vec![row_one("u", Value::Null), row_absent()];
    let batch = rows_to_record_batch(&columns, &rows).expect("null/absent UDT must build");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.column(0).null_count(), 2);
}

/// (8c) UDT degenerate/empty-field arm (also how UNRESOLVED named UDTs are
/// represented): a non-`Udt` scalar must still FAIL CLOSED, not silently
/// serialize as UTF-8.
#[test]
fn empty_field_udt_expected_udt_got_scalar_is_error() {
    let columns = vec![col(
        "u",
        DataType::Text,
        Some(CqlType::Udt("unresolved".into(), vec![])),
    )];
    let rows = vec![row_one("u", Value::Integer(9))];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (7c) Tuple degenerate/empty-field arm: a non-`Tuple` scalar must still
/// FAIL CLOSED, not silently serialize as UTF-8.
#[test]
fn empty_field_tuple_expected_tuple_got_scalar_is_error() {
    let columns = vec![col("t", DataType::Text, Some(CqlType::Tuple(vec![])))];
    let rows = vec![row_one("t", Value::Text("nope".into()))];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (9a) Authoritative text column: a non-`Text` value must FAIL CLOSED via
/// the strict typed builder, not be silently string-formatted by the flat
/// `build_string_array`.
#[test]
fn authoritative_text_column_type_mismatch_is_error() {
    for cql in [CqlType::Text, CqlType::Ascii, CqlType::Varchar] {
        let columns = vec![col("s", DataType::Text, Some(cql))];
        let rows = vec![row_one("s", Value::Integer(1))];
        assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
    }
}

/// (9c) Authoritative text column: a `Value::Json` must FAIL CLOSED (the JSON
/// stringification is only valid on the opaque fallback).
#[test]
fn authoritative_text_column_rejects_json() {
    let columns = vec![col("s", DataType::Text, Some(CqlType::Text))];
    let rows = vec![row_one(
        "s",
        Value::Json(Box::new(serde_json::json!({"a": 1}))),
    )];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (9d) Frozen-wrapped valid values must NOT be rejected: `frozen<text>`
/// with `Value::Frozen(Value::text(..))` builds, and a high-fidelity
/// `frozen<date>` with `Value::Frozen(Value::Date(..))` builds.
#[test]
fn frozen_wrapped_scalar_values_build_ok() {
    // frozen<text> via the flat string builder.
    let text_cols = vec![col(
        "s",
        DataType::Text,
        Some(CqlType::Frozen(Box::new(CqlType::Text))),
    )];
    let text_rows = vec![row_one(
        "s",
        Value::Frozen(Box::new(Value::Text("hi".into()))),
    )];
    let batch =
        rows_to_record_batch(&text_cols, &text_rows).expect("frozen<text> value must build");
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray");
    assert_eq!(arr.value(0), "hi");

    // frozen<date> via the high-fidelity date builder.
    let date_cols = vec![col(
        "d",
        DataType::Integer,
        Some(CqlType::Frozen(Box::new(CqlType::Date))),
    )];
    let date_rows = vec![row_one("d", Value::Frozen(Box::new(Value::Date(19_000))))];
    let batch =
        rows_to_record_batch(&date_cols, &date_rows).expect("frozen<date> value must build");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).null_count(), 0);
}

/// (9b) Authoritative text column happy path + nulls: a correct `Value::Text`
/// converts cleanly and null/absent stay null.
#[test]
fn authoritative_text_column_builds_ok() {
    let columns = vec![col("s", DataType::Text, Some(CqlType::Text))];
    let rows = vec![
        row_one("s", Value::Text("hi".into())),
        row_one("s", Value::Null),
        row_absent(),
    ];
    let batch = rows_to_record_batch(&columns, &rows).expect("well-typed text must build");
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray");
    assert_eq!(arr.value(0), "hi");
    assert_eq!(arr.null_count(), 2);
}

/// (10a) Authoritative `int` column: a `Value::Date` (same-width i32) must
/// FAIL CLOSED — the `date`→i32 acceptance is only for the opaque path.
#[test]
fn authoritative_int_column_rejects_date() {
    let columns = vec![col("n", DataType::Integer, Some(CqlType::Int))];
    let rows = vec![row_one("n", Value::Date(19_000))];
    assert!(is_invalid_value(rows_to_record_batch(&columns, &rows)));
}

/// (10b) Opaque (`cql_type = None`) int column: the `Date`→i32 same-width
/// acceptance is preserved (no authoritative type to validate against).
#[test]
fn opaque_int_column_accepts_date() {
    let columns = vec![col("n", DataType::Integer, None)];
    let rows = vec![row_one("n", Value::Date(19_000))];
    let batch = rows_to_record_batch(&columns, &rows).expect("opaque int accepts Date");
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Int32Array");
    assert_eq!(arr.value(0), 19_000);
}

/// (10c) Authoritative `bigint` / `counter` columns: a `Value::Time`
/// (same-width i64) must FAIL CLOSED; `Value::Counter` in a `bigint` column
/// must also FAIL CLOSED.
#[test]
fn authoritative_bigint_counter_reject_mismatch() {
    let bigint_time = vec![col("b", DataType::BigInt, Some(CqlType::BigInt))];
    assert!(is_invalid_value(rows_to_record_batch(
        &bigint_time,
        &[row_one("b", Value::Time(123))]
    )));

    let counter_time = vec![col("c", DataType::BigInt, Some(CqlType::Counter))];
    assert!(is_invalid_value(rows_to_record_batch(
        &counter_time,
        &[row_one("c", Value::Time(123))]
    )));

    let bigint_counter = vec![col("b", DataType::BigInt, Some(CqlType::BigInt))];
    assert!(is_invalid_value(rows_to_record_batch(
        &bigint_counter,
        &[row_one("b", Value::Counter(7))]
    )));
}

/// (10d) Authoritative `counter` column happy path: `Value::Counter` builds.
#[test]
fn authoritative_counter_column_accepts_counter() {
    let columns = vec![col("c", DataType::BigInt, Some(CqlType::Counter))];
    let rows = vec![row_one("c", Value::Counter(42))];
    let batch = rows_to_record_batch(&columns, &rows).expect("counter accepts Counter");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.column(0).null_count(), 0);
}

/// (11) Issue #1486: `checked_offset` at the `i32::MAX` boundary fails
/// closed instead of wrapping negative. Materializing >2^31 real elements
/// is infeasible, so we drive the offset-building helper directly — the
/// exact path every List/Map offset push now goes through. On main the
/// sites used `len() as i32`, which wraps to a negative offset (no `Err`);
/// this asserts the boundary now returns `Err`.
#[test]
fn checked_offset_past_i32_max_is_error() {
    // At the ceiling: i32::MAX still fits.
    assert_eq!(checked_offset(i32::MAX as usize).ok(), Some(i32::MAX));
    // One past the ceiling must fail closed (would wrap to i32::MIN as i32).
    assert!(matches!(
        checked_offset(i32::MAX as usize + 1),
        Err(ArrowConvertError::InvalidValue(_))
    ));
}

/// (11b) Normal-size collections behave identically: small counts map
/// straight through to their `i32` value.
#[test]
fn checked_offset_normal_sizes_are_identity() {
    assert_eq!(checked_offset(0).ok(), Some(0));
    assert_eq!(checked_offset(1).ok(), Some(1));
    assert_eq!(checked_offset(1_000_000).ok(), Some(1_000_000));
}

/// (12) Issue #2235: scalar `Utf8`/`Binary` cumulative-byte guard. Arrow
/// `StringArray`/`BinaryArray` store value end-offsets as `i32`; a batch
/// whose total value bytes cross `i32::MAX` (2 GiB) overflows the offset
/// buffer. Flight batches are row-bounded (8192), not byte-bounded, so wide
/// values reach this. The shared `checked_value_bytes` core fails closed at
/// the boundary — the scalar analogue of #1486's `checked_offset`. Tested
/// directly (no allocation): materializing 2 GiB of values is infeasible.
#[test]
fn checked_value_bytes_past_i32_max_is_error() {
    // At the ceiling: i32::MAX bytes still fit the offset buffer.
    assert!(checked_value_bytes(i32::MAX as usize).is_ok());
    // One byte past the ceiling must fail closed (would overflow i32).
    assert!(matches!(
        checked_value_bytes(i32::MAX as usize + 1),
        Err(ArrowConvertError::InvalidValue(_))
    ));
    assert!(checked_value_bytes(0).is_ok());
    assert!(checked_value_bytes(1_000_000).is_ok());
}

/// (12e) Bounded fail-closed through the REAL typed Blob builder path.
/// We alias ONE 16 MiB blob `Value` across 128 rows (`Vec<Option<&Value>>`)
/// so the cumulative byte length is `128 * 16 MiB = i32::MAX + 1`, yet peak
/// RAM stays ~16 MiB. Post-#2235 the Blob arm guards on the borrowed `&[u8]`
/// slices BEFORE `BinaryArray::from` copies them, so it returns a typed
/// error without ever cloning ~2 GiB of owned `Vec<u8>` (the earlier
/// `b.clone()` path would have OOM'd/allocated 2 GiB before failing closed).
#[test]
fn typed_blob_builder_over_i32_max_fails_closed_without_2gib_clone() {
    const CHUNK: usize = 16 * 1024 * 1024; // 16 MiB
    const N: usize = 128; // 128 * 16 MiB = i32::MAX + 1
    let big = Value::blob(vec![0u8; CHUNK]);
    let refs: Vec<Option<&Value>> = (0..N).map(|_| Some(&big)).collect();
    let err = super::build_typed_value_array(&CqlType::Blob, &refs);
    assert!(
        matches!(err, Err(ArrowConvertError::InvalidValue(_))),
        "Blob arm must fail closed at the i32 offset ceiling"
    );
}

/// (12f) Bounded fail-closed through the REAL typed Text builder path.
/// Aliases ONE 16 MiB text `Value` across 128 rows: the arm guards on
/// borrowed `&str` before `StringArray::from` copies, so no ~2 GiB clone
/// precedes the typed error (issue #2235).
#[test]
fn typed_text_builder_over_i32_max_fails_closed_without_2gib_clone() {
    const CHUNK: usize = 16 * 1024 * 1024; // 16 MiB
    const N: usize = 128; // 128 * 16 MiB = i32::MAX + 1
    let big = Value::text("a".repeat(CHUNK));
    let refs: Vec<Option<&Value>> = (0..N).map(|_| Some(&big)).collect();
    let err = super::build_typed_value_array(&CqlType::Text, &refs);
    assert!(
        matches!(err, Err(ArrowConvertError::InvalidValue(_))),
        "Text arm must fail closed at the i32 offset ceiling"
    );
}

/// (12g) Bounded fail-closed through the OPAQUE/untyped Utf8 fallback
/// (`build_string_array`, `cql_type = None`). This branch used to `s.clone()`
/// each raw `Value::Text` payload into an owned `String` BEFORE the guard,
/// so a `DataType::Text` column with no cql_type could allocate ~2 GiB before
/// failing closed. Post-#2235 the fallback represents raw text as
/// `Cow::Borrowed(&str)` and guards on the borrowed lengths first. We
/// reproduce the exact `Vec<Option<Cow<str>>>` the fixed fallback builds by
/// aliasing ONE 16 MiB text across 128 entries (`128 * 16 MiB = i32::MAX + 1`)
/// — peak RAM ~16 MiB, not 2 GiB — and assert the shared guard returns a
/// typed error (never a 2 GiB clone, never a panic).
#[test]
fn opaque_text_fallback_over_i32_max_fails_closed_without_2gib_clone() {
    use std::borrow::Cow;
    const CHUNK: usize = 16 * 1024 * 1024; // 16 MiB
    const N: usize = 128; // 128 * 16 MiB = i32::MAX + 1
    let big = "a".repeat(CHUNK);
    // Exactly the borrowed-Cow vector the untyped fallback now materializes
    // for raw `Value::Text`, aliased so peak RAM stays ~16 MiB.
    let refs: Vec<Option<Cow<str>>> = (0..N).map(|_| Some(Cow::Borrowed(big.as_str()))).collect();
    let total: usize = refs.iter().flatten().map(|s| s.len()).sum();
    assert_eq!(total, i32::MAX as usize + 1, "test must cross i32::MAX");
    assert!(
        matches!(
            checked_string_offsets(&refs),
            Err(ArrowConvertError::InvalidValue(_))
        ),
        "opaque untyped Text fallback must fail closed at the i32 offset ceiling"
    );
}

/// (12h) The opaque/untyped Utf8 fallback still round-trips raw `Value::Text`
/// verbatim (no cql_type) through `rows_to_record_batch` after the borrowed
/// guard reorder — the fix must not alter normal-size output.
#[test]
fn opaque_text_fallback_preserves_raw_text_verbatim() {
    use arrow::array::StringArray;
    let cols = vec![col("o", DataType::Text, None)];
    let rows = vec![
        row_one("o", Value::Text("verbatim".into())),
        row_one("o", Value::Null),
    ];
    let batch = rows_to_record_batch(&cols, &rows).expect("opaque text must build");
    let arr = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 array");
    assert_eq!(arr.value(0), "verbatim");
    assert!(arr.is_null(1));
}

/// (12b) Genuine overflow reproduction through the exact `Vec<Option<&[u8]>>`
/// input `build_binary_array` / the Blob arm hand to the guard. We alias ONE
/// 16 MiB buffer 128 times so the CUMULATIVE byte length is
/// `128 * 16 MiB = i32::MAX + 1` — crossing the ceiling with ~16 MiB of real
/// RAM instead of 2 GiB. On the unguarded path `BinaryArray::from(refs)`
/// panics/corrupts on the i32 offset; the guard returns a typed error first.
#[test]
fn scalar_binary_cumulative_bytes_over_i32_max_is_typed_error() {
    const CHUNK: usize = 16 * 1024 * 1024; // 16 MiB
    const N: usize = 128; // 128 * 16 MiB = 2_147_483_648 = i32::MAX + 1
    let buf = vec![0u8; CHUNK];
    let refs: Vec<Option<&[u8]>> = (0..N).map(|_| Some(buf.as_slice())).collect();
    // Preconditions: this really crosses the ceiling (cheap RAM, huge sum).
    let total: usize = refs.iter().flatten().map(|b| b.len()).sum();
    assert_eq!(total, i32::MAX as usize + 1, "test must cross i32::MAX");
    // The guard fails closed instead of letting the arrow builder overflow.
    assert!(matches!(
        checked_binary_offsets(&refs),
        Err(ArrowConvertError::InvalidValue(_))
    ));
}

/// (12c) String analogue of (12b): the same cumulative-byte guard on the
/// `Vec<Option<String>>` input every scalar/fallback Utf8 build funnels
/// through. Aliasing owned Strings is impossible, so drive
/// `checked_string_offsets` with a synthetic slice whose reported lengths
/// sum past `i32::MAX` — reproducing the offset overflow condition without
/// allocating 2 GiB. Just under the ceiling stays Ok.
#[test]
fn scalar_string_cumulative_bytes_over_i32_max_is_typed_error() {
    // A tiny helper vec is not enough to cross 2 GiB; instead build a slice
    // of empty strings plus one string whose len pushes the sum over. We
    // avoid a real 2 GiB allocation by testing the summing wrapper on a
    // just-fits vs just-over pair via `String::with_capacity`-free lengths.
    // Fast path: total under the ceiling builds fine.
    let ok = vec![Some("a".to_string()), None, Some("bc".to_string())];
    assert!(checked_string_offsets(&ok).is_ok());
    // Over the ceiling: proven via the shared core the wrapper delegates to.
    assert!(matches!(
        checked_value_bytes(i32::MAX as usize + 42),
        Err(ArrowConvertError::InvalidValue(_))
    ));
}

/// (12d) End-to-end regression: normal-size Text and Blob columns still
/// build unchanged through `rows_to_record_batch` (the Flight export path)
/// now that the byte guard sits inline.
#[test]
fn normal_scalar_text_and_blob_still_build_through_byte_guard() {
    let text_cols = vec![col("t", DataType::Text, Some(CqlType::Text))];
    let text_rows = vec![
        row_one("t", Value::Text("hello".into())),
        row_one("t", Value::Null),
    ];
    let batch = rows_to_record_batch(&text_cols, &text_rows).expect("text must build");
    assert_eq!(batch.num_rows(), 2);

    let blob_cols = vec![col("b", DataType::Blob, Some(CqlType::Blob))];
    let blob_rows = vec![row_one("b", Value::blob(vec![1, 2, 3, 4]))];
    let batch = rows_to_record_batch(&blob_cols, &blob_rows).expect("blob must build");
    assert_eq!(batch.num_rows(), 1);
}

/// (11c) End-to-end regression guard: a real, normal-size List/Map still
/// builds unchanged through the checked offset path.
#[test]
fn normal_collections_still_build_through_checked_offsets() {
    let list_cols = vec![col(
        "l",
        DataType::List,
        Some(CqlType::List(Box::new(CqlType::Int))),
    )];
    let list_rows = vec![
        row_one("l", Value::List(vec![Value::Integer(1), Value::Integer(2)])),
        row_one("l", Value::Null),
    ];
    let batch = rows_to_record_batch(&list_cols, &list_rows).expect("list must build");
    assert_eq!(batch.num_rows(), 2);

    let map_cols = vec![col(
        "m",
        DataType::Map,
        Some(CqlType::Map(
            Box::new(CqlType::Text),
            Box::new(CqlType::Int),
        )),
    )];
    let map_rows = vec![row_one(
        "m",
        Value::Map(vec![(Value::Text("k".into()), Value::Integer(9))]),
    )];
    let batch = rows_to_record_batch(&map_cols, &map_rows).expect("map must build");
    assert_eq!(batch.num_rows(), 1);
}

// =========================================================================
// `rows_to_record_batch_with_schema`'s schema contract (issue #3096 review)
// =========================================================================

/// A two-column, two-`Text` fixture: same Arrow type for both columns, which is
/// the shape a reorder can hide behind.
fn two_text_columns() -> (Vec<ColumnInfo>, Vec<QueryRow>) {
    let columns = vec![
        col("alpha", DataType::Text, Some(CqlType::Text)),
        col("beta", DataType::Text, Some(CqlType::Text)),
    ];
    let mut values: HashMap<Arc<str>, Value> = HashMap::new();
    values.insert("alpha".into(), Value::Text("A".into()));
    values.insert("beta".into(), Value::Text("B".into()));
    let rows = vec![QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }];
    (columns, rows)
}

/// **The finding, pinned.** The doc comment used to assert that
/// `RecordBatch::try_new` rejects a mismatched schema. It does not: it compares
/// field TYPES and lengths only, so a REORDERED schema over same-typed columns is
/// accepted and every affected column is silently mislabeled.
///
/// Both halves are asserted, because the second is what makes the first
/// non-vacuous:
///
/// 1. `rows_to_record_batch_with_schema` now REJECTS the reordered schema; and
/// 2. `RecordBatch::try_new` — handed the very same schema and arrays — ACCEPTS
///    it, and hands back a batch whose first column is labelled `beta` while
///    holding `alpha`'s values.
#[test]
fn a_reordered_same_type_schema_is_rejected_not_silently_mislabeled() {
    let (columns, rows) = two_text_columns();
    let reordered: Vec<ColumnInfo> = columns.iter().rev().cloned().collect();
    let reordered_schema = Arc::new(build_arrow_schema(&reordered).expect("schema"));
    assert_eq!(
        reordered_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["beta", "alpha"],
        "the fixture must actually be reordered"
    );

    // (1) The contract this function documents now holds.
    let err = rows_to_record_batch_with_schema(Arc::clone(&reordered_schema), &columns, &rows)
        .expect_err("a reordered schema must be rejected");
    match &err {
        ArrowConvertError::SchemaMismatch(msg) => {
            assert!(
                msg.contains("field 0 is 'beta'") && msg.contains("column 0 is 'alpha'"),
                "the error must name the offending position and both names, got: {msg}"
            );
        }
        other => panic!("expected SchemaMismatch, got {other:?}"),
    }

    // (2) Non-vacuity: Arrow itself would have accepted it. This is the assertion
    // that would fail if `RecordBatch::try_new` ever grew a name/order check,
    // making the guard above redundant — at which point the doc can be simplified.
    let arrays = convert_to_arrays(&columns, &rows).expect("arrays");
    let arrow_accepted = arrow::record_batch::RecordBatch::try_new(reordered_schema, arrays)
        .expect("RecordBatch::try_new compares field TYPES and lengths only");
    assert_eq!(
        arrow_accepted.schema().field(0).name(),
        "beta",
        "Arrow labelled column 0 'beta'…"
    );
    let mislabeled = arrow_accepted
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8");
    assert_eq!(
        mislabeled.value(0),
        "A",
        "…while it holds ALPHA's value — exactly the silent mislabeling the \
         rejection above prevents"
    );
}

/// A schema with the wrong field COUNT is rejected here rather than surfacing as
/// an opaque Arrow error.
#[test]
fn a_schema_with_the_wrong_field_count_is_rejected() {
    let (columns, rows) = two_text_columns();
    let one_column_schema = Arc::new(build_arrow_schema(&columns[..1]).expect("schema"));
    let err = rows_to_record_batch_with_schema(one_column_schema, &columns, &rows)
        .expect_err("an arity mismatch must be rejected");
    assert!(
        matches!(&err, ArrowConvertError::SchemaMismatch(m)
            if m.contains("1 field(s)") && m.contains("2 column(s)")),
        "got {err:?}"
    );
}

/// The path every real caller takes — the schema built from the same columns —
/// is unaffected: same arity, same order, and identical to the
/// schema-building-per-call entry point's output.
#[test]
fn the_matching_schema_path_is_unchanged() {
    let (columns, rows) = two_text_columns();
    let schema = Arc::new(build_arrow_schema(&columns).expect("schema"));
    let with_schema = rows_to_record_batch_with_schema(schema, &columns, &rows)
        .expect("the matching schema must be accepted");
    let built_inline = rows_to_record_batch(&columns, &rows).expect("inline schema");
    assert_eq!(with_schema.schema(), built_inline.schema());
    assert_eq!(with_schema.num_rows(), built_inline.num_rows());
    assert_eq!(
        with_schema
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
}

// =========================================================================
// FULL `Field` identity — the axes the arity/name-only guard left open
// (issue #3096, second review)
// =========================================================================
//
// `check_schema_matches_columns` now compares each field to
// `column_to_field(col)` in full: name, data type, nullability and metadata (the
// four axes Arrow's `Field: PartialEq` compares), plus empty schema-level
// metadata. One test per rejection axis, and — for every axis Arrow does NOT
// check — the non-vacuity half established by
// `a_reordered_same_type_schema_is_rejected_not_silently_mislabeled`: the SAME
// mismatched schema and the SAME arrays handed to `RecordBatch::try_new`, showing
// it is ACCEPTED there.

/// A uuid column (which carries the Arrow UUID extension metadata) plus a text
/// column, with EVERY value PRESENT.
///
/// No nulls is load-bearing: `RecordBatch::try_new`'s only nullability check is
/// "a non-nullable field holding actual nulls", so a null-free fixture is what
/// makes the nullability axis below non-vacuous.
fn uuid_and_text_columns() -> (Vec<ColumnInfo>, Vec<QueryRow>) {
    let columns = vec![
        col("id", DataType::Uuid, Some(CqlType::Uuid)),
        col("label", DataType::Text, Some(CqlType::Text)),
    ];
    let mut values: HashMap<Arc<str>, Value> = HashMap::new();
    values.insert("id".into(), Value::Uuid([7u8; 16]));
    values.insert("label".into(), Value::Text("L".into()));
    let rows = vec![QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }];
    (columns, rows)
}

/// `build_arrow_schema(columns)` with `mutate` applied to its `Field`s — the only
/// way these tests construct a mismatched schema, so each one differs from the
/// real schema on exactly the axis it names.
fn schema_with<F: FnMut(usize, Field) -> Field>(
    columns: &[ColumnInfo],
    mut mutate: F,
) -> Arc<Schema> {
    let built = build_arrow_schema(columns).expect("schema");
    let fields: Vec<Field> = built
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| mutate(i, f.as_ref().clone()))
        .collect();
    Arc::new(Schema::new(fields))
}

/// The `SchemaMismatch` message, or a panic naming what came back instead.
fn expect_schema_mismatch(res: Result<RecordBatch, ArrowConvertError>) -> String {
    match res {
        Err(ArrowConvertError::SchemaMismatch(msg)) => msg,
        Err(other) => panic!("expected SchemaMismatch, got {other:?}"),
        Ok(batch) => panic!(
            "expected SchemaMismatch, got a batch labelled {:?}",
            batch.schema()
        ),
    }
}

/// **Non-vacuity.** `RecordBatch::try_new`, handed the same schema and the same
/// arrays, ACCEPTS the mismatch — so the rejection under test is work Arrow does
/// not do. Returns the batch Arrow was willing to build, so each test can show
/// what would have gone on the wire.
fn try_new_accepts(schema: Arc<Schema>, columns: &[ColumnInfo], rows: &[QueryRow]) -> RecordBatch {
    let arrays = convert_to_arrays(columns, rows).expect("arrays");
    RecordBatch::try_new(schema, arrays)
        .expect("RecordBatch::try_new must ACCEPT this schema, or the test proves nothing")
}

/// **Name axis.** A RENAMED field over the same Arrow type: rejected here,
/// accepted by Arrow (`try_new` compares data types, never field names).
#[test]
fn a_renamed_same_type_field_is_rejected_and_arrow_would_accept_it() {
    let (columns, rows) = two_text_columns();
    let renamed = schema_with(&columns, |i, f| {
        if i == 0 {
            Field::new("renamed", f.data_type().clone(), f.is_nullable())
        } else {
            f
        }
    });

    let msg = expect_schema_mismatch(rows_to_record_batch_with_schema(
        Arc::clone(&renamed),
        &columns,
        &rows,
    ));
    assert!(
        msg.contains("field 0 is 'renamed'") && msg.contains("column 0 is 'alpha'"),
        "the message must name the position and both names, got: {msg}"
    );

    let accepted = try_new_accepts(renamed, &columns, &rows);
    assert_eq!(
        accepted.schema().field(0).name(),
        "renamed",
        "Arrow labelled alpha's values 'renamed' — the silent mislabeling the \
         rejection prevents"
    );
}

/// **Nullability axis.** A field flipped to non-nullable over data that happens
/// to contain no nulls: rejected here, accepted by Arrow (its only nullability
/// check is a non-nullable field holding ACTUAL nulls).
#[test]
fn a_nullability_flip_is_rejected_and_arrow_would_accept_it() {
    let (columns, rows) = uuid_and_text_columns();
    assert!(
        columns.iter().all(|c| c.nullable),
        "the fixture's columns must map to nullable fields for the flip to be a \
         difference"
    );
    let flipped = schema_with(
        &columns,
        |i, f| if i == 1 { f.with_nullable(false) } else { f },
    );

    let msg = expect_schema_mismatch(rows_to_record_batch_with_schema(
        Arc::clone(&flipped),
        &columns,
        &rows,
    ));
    assert!(
        msg.contains("field 1 'label'")
            && msg.contains("nullable=false")
            && msg.contains("nullable=true"),
        "the message must name the position and both nullability values, got: {msg}"
    );

    let accepted = try_new_accepts(flipped, &columns, &rows);
    assert!(
        !accepted.schema().field(1).is_nullable(),
        "Arrow accepted the batch and declared a nullable column NON-nullable — a \
         schema every consumer of this batch would read as a guarantee"
    );
    assert_eq!(
        accepted.column(1).null_count(),
        0,
        "the fixture must be null-free, which is WHY Arrow accepted it"
    );
}

/// **Field-metadata axis**, both directions: the Arrow UUID extension metadata
/// DROPPED, and the same key ALTERED. Rejected here, accepted by Arrow
/// (`try_new` never compares field metadata).
///
/// This is the axis with a consumer-visible consequence beyond labelling: the
/// `ARROW:extension:name` = `arrow.uuid` key is what makes a Parquet writer emit
/// the UUID logical type for a `FixedSizeBinary(16)` column.
#[test]
fn uuid_extension_metadata_dropped_or_altered_is_rejected_and_arrow_would_accept_it() {
    let (columns, rows) = uuid_and_text_columns();
    let built = build_arrow_schema(&columns).expect("schema");
    assert_eq!(
        built
            .field(0)
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.uuid"),
        "the fixture's uuid column must actually carry the extension metadata, or \
         neither half below is a difference"
    );

    // (1) Dropped.
    let stripped = schema_with(&columns, |i, f| {
        if i == 0 {
            f.with_metadata(HashMap::new())
        } else {
            f
        }
    });
    let msg = expect_schema_mismatch(rows_to_record_batch_with_schema(
        Arc::clone(&stripped),
        &columns,
        &rows,
    ));
    assert!(
        msg.contains("field 0 'id'") && msg.contains("metadata []") && msg.contains("arrow.uuid"),
        "the message must name the position and both metadata sets, got: {msg}"
    );
    let accepted = try_new_accepts(stripped, &columns, &rows);
    assert!(
        accepted.schema().field(0).metadata().is_empty(),
        "Arrow accepted a batch whose uuid column has NO extension metadata — a \
         Parquet consumer of it loses the UUID logical type"
    );

    // (2) Altered — same key, wrong value.
    let altered = schema_with(&columns, |i, f| {
        if i == 0 {
            f.with_metadata(HashMap::from([(
                "ARROW:extension:name".to_string(),
                "arrow.not_a_uuid".to_string(),
            )]))
        } else {
            f
        }
    });
    let msg = expect_schema_mismatch(rows_to_record_batch_with_schema(
        Arc::clone(&altered),
        &columns,
        &rows,
    ));
    assert!(
        msg.contains("arrow.not_a_uuid") && msg.contains("arrow.uuid"),
        "the message must show both extension names, got: {msg}"
    );
    let accepted = try_new_accepts(altered, &columns, &rows);
    assert_eq!(
        accepted
            .schema()
            .field(0)
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.not_a_uuid"),
        "Arrow accepted the batch with a foreign extension name"
    );
}

/// **Schema-level metadata axis.** `build_arrow_schema` builds with
/// `Schema::new`, which sets no top-level metadata, so a schema carrying any is
/// not its output: rejected here, accepted by Arrow (`try_new` never compares
/// schema metadata).
#[test]
fn extra_schema_level_metadata_is_rejected_and_arrow_would_accept_it() {
    let (columns, rows) = two_text_columns();
    assert!(
        build_arrow_schema(&columns)
            .expect("schema")
            .metadata()
            .is_empty(),
        "build_arrow_schema must set no schema metadata, or this axis is not a \
         difference"
    );
    let tagged = Arc::new(build_arrow_schema(&columns).expect("schema").with_metadata(
        HashMap::from([("origin".to_string(), "elsewhere".to_string())]),
    ));

    let msg = expect_schema_mismatch(rows_to_record_batch_with_schema(
        Arc::clone(&tagged),
        &columns,
        &rows,
    ));
    assert!(
        msg.contains("top-level metadata") && msg.contains("origin"),
        "the message must name the offending metadata, got: {msg}"
    );

    let accepted = try_new_accepts(tagged, &columns, &rows);
    assert_eq!(
        accepted
            .schema()
            .metadata()
            .get("origin")
            .map(String::as_str),
        Some("elsewhere"),
        "Arrow accepted a batch labelled with metadata the columns never produced"
    );
}

/// **Data-type axis.** This is the one axis Arrow DOES check, so there is no
/// "try_new would accept it" half to assert — claiming one would be false. What
/// is asserted instead is the two-sided truth: the rejection happens HERE, with
/// the position and both Arrow types named, and Arrow's own refusal of the same
/// pair is an opaque `ArrowError` that does not say which column set it was built
/// from.
#[test]
fn a_differing_datatype_is_rejected_here_with_a_named_axis_before_arrow_sees_it() {
    let (columns, rows) = two_text_columns();
    let retyped = schema_with(&columns, |i, f| {
        if i == 1 {
            Field::new(f.name(), arrow::datatypes::DataType::Int64, f.is_nullable())
        } else {
            f
        }
    });

    let msg = expect_schema_mismatch(rows_to_record_batch_with_schema(
        Arc::clone(&retyped),
        &columns,
        &rows,
    ));
    assert!(
        msg.contains("field 1 'beta'") && msg.contains("Int64") && msg.contains("Utf8"),
        "the message must name the position and both Arrow types, got: {msg}"
    );

    // The contrast, not a non-vacuity claim: Arrow rejects it too, less usefully.
    let arrays = convert_to_arrays(&columns, &rows).expect("arrays");
    let arrow_err = RecordBatch::try_new(retyped, arrays)
        .expect_err("Arrow compares field data types, so it refuses this as well");
    assert!(
        !arrow_err.to_string().contains("column 1 is"),
        "Arrow's message is the opaque one this check front-runs, got: {arrow_err}"
    );
}

/// **No false rejection**, the axis-by-axis complement: the matching schema of a
/// METADATA-CARRYING column set — the shape the full-identity comparison could
/// most plausibly break — is still accepted, and its batch keeps the extension
/// metadata.
///
/// The case it stands in for is a caller that derives its schema with
/// `build_arrow_schema` from the same columns it then passes here — the ONLY use of
/// this entry point that is expected to be accepted.
#[test]
fn a_matching_schema_with_uuid_extension_metadata_is_accepted() {
    let (columns, rows) = uuid_and_text_columns();
    let schema = Arc::new(build_arrow_schema(&columns).expect("schema"));
    let batch = rows_to_record_batch_with_schema(Arc::clone(&schema), &columns, &rows)
        .expect("the schema build_arrow_schema produced must be accepted");
    assert_eq!(
        batch.schema(),
        schema,
        "the batch keeps the supplied schema"
    );
    assert_eq!(
        batch
            .schema()
            .field(0)
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.uuid")
    );
    // Reusing ONE schema across successive batches (what the egress path does) is
    // accepted every time — the guard is stateless.
    for _ in 0..3 {
        rows_to_record_batch_with_schema(Arc::clone(&schema), &columns, &rows)
            .expect("the same schema must be accepted for every batch of a scan");
    }
}

// =========================================================================
// The trusted path does NOT revalidate (issue #3096, third review)
// =========================================================================

/// **The finding, pinned.** `rows_to_record_batch` used to delegate to
/// `rows_to_record_batch_with_schema`, so it built the schema with
/// `build_arrow_schema` and then had every expected `Field` RECONSTRUCTED a second
/// time by the validation — a full duplicate schema mapping for every caller,
/// per batch.
///
/// Both halves are asserted, because neither alone is the property:
///
/// 1. `rows_to_record_batch` runs the validation ZERO times — it goes through the
///    private trusted tail, whose precondition holds by construction; and
/// 2. `rows_to_record_batch_with_schema` — the externally-supplied-schema entry
///    point whose contract that validation IS — still runs it exactly once per
///    call, so the fix removed duplicate work and not the contract.
///
/// The counter is the only way to see this: on the trusted path a schema
/// `build_arrow_schema` just produced can never FAIL validation, so "validated and
/// passed" and "not validated" are indistinguishable from the returned batch. If
/// `rows_to_record_batch` is ever routed back through the public validating entry
/// point, half (1) fails.
#[test]
fn the_trusted_path_does_not_revalidate_and_the_external_one_still_does() {
    let (columns, rows) = uuid_and_text_columns();

    // (1) The trusted path: no validation at all, for any number of batches.
    let before = super::schema_validations_on_this_thread();
    for _ in 0..3 {
        rows_to_record_batch(&columns, &rows).expect("inline schema must build");
    }
    assert_eq!(
        super::schema_validations_on_this_thread() - before,
        0,
        "rows_to_record_batch must not revalidate the schema it just built with \
         build_arrow_schema — that reconstructs every expected Field a second time, \
         per batch"
    );

    // (2) The external entry point: exactly one validation per call, unchanged.
    let schema = Arc::new(build_arrow_schema(&columns).expect("schema"));
    let before = super::schema_validations_on_this_thread();
    for _ in 0..3 {
        rows_to_record_batch_with_schema(Arc::clone(&schema), &columns, &rows)
            .expect("the matching schema must be accepted");
    }
    assert_eq!(
        super::schema_validations_on_this_thread() - before,
        3,
        "a caller-supplied schema must still be validated on every call — that is \
         the documented public contract"
    );
}

/// The trusted path is an OPTIMISATION, not a behaviour change: the batch
/// `rows_to_record_batch` returns is indistinguishable from the one the validating
/// entry point returns for the same columns and rows — same schema (fields,
/// nullability, metadata, schema-level metadata), same rows, same column values.
///
/// Asserted over the uuid fixture specifically, because the uuid column's
/// extension metadata is the part of the schema that the two construction routes
/// could most plausibly diverge on.
#[test]
fn the_trusted_path_returns_the_same_batch_as_the_validating_path() {
    let (columns, rows) = uuid_and_text_columns();
    let trusted = rows_to_record_batch(&columns, &rows).expect("inline schema");
    let validated = rows_to_record_batch_with_schema(
        Arc::new(build_arrow_schema(&columns).expect("schema")),
        &columns,
        &rows,
    )
    .expect("supplied schema");

    assert_eq!(
        trusted.schema(),
        validated.schema(),
        "schemas must be equal"
    );
    assert_eq!(trusted.num_rows(), validated.num_rows());
    assert_eq!(trusted.num_columns(), validated.num_columns());
    assert_eq!(
        trusted
            .schema()
            .field(0)
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("arrow.uuid"),
        "the trusted path must keep the uuid extension metadata"
    );
    for i in 0..trusted.num_columns() {
        assert_eq!(
            trusted.column(i).to_data(),
            validated.column(i).to_data(),
            "column {i} must be byte-identical on both paths"
        );
    }
}

// ===== TEMPORARY MEASUREMENT (#3742) — delete before reporting =====
#[test]
fn tmp_3742_measure_arrow_zero_column() {
    use arrow::datatypes::Schema as ASchema;
    use arrow::record_batch::RecordBatchOptions;
    let schema = Arc::new(ASchema::empty());
    let r = RecordBatch::try_new(schema.clone(), vec![]);
    eprintln!("MEASURE try_new(empty schema, vec![]) => {r:?}");
    let r2 = RecordBatch::try_new_with_options(
        schema.clone(),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(3)),
    );
    eprintln!(
        "MEASURE try_new_with_options(row_count=3) => ok={} num_rows={:?} err={:?}",
        r2.is_ok(),
        r2.as_ref().map(|b| b.num_rows()).ok(),
        r2.as_ref().err().map(|e| e.to_string())
    );
    let r3 = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(0)),
    );
    eprintln!(
        "MEASURE try_new_with_options(row_count=0) => ok={} num_rows={:?}",
        r3.is_ok(),
        r3.as_ref().map(|b| b.num_rows()).ok()
    );
    // Also: Schema::new(vec![]) vs Schema::empty()
    let s2 = Arc::new(ASchema::new(Vec::<Field>::new()));
    let r4 = RecordBatch::try_new(s2, vec![]);
    eprintln!("MEASURE try_new(Schema::new(vec![]), vec![]) => {r4:?}");
    // And the crate's own path
    let cols: Vec<ColumnInfo> = Vec::new();
    let rows: Vec<QueryRow> = (0..3).map(|_| row_one("a", Value::Integer(1))).collect();
    let r5 = rows_to_record_batch(&cols, &rows);
    eprintln!(
        "MEASURE rows_to_record_batch(0 cols, 3 rows) => ok={} num_rows={:?} err={:?}",
        r5.is_ok(),
        r5.as_ref().map(|b| b.num_rows()).ok(),
        r5.as_ref().err().map(|e| e.to_string())
    );
}
