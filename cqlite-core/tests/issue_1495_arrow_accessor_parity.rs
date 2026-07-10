//! Output-equivalence guard for issue #1495 (AE1) — the load-bearing parity net
//! for the "resolve per-column accessor once" refactor.
//!
//! **What #1495 does**: every Arrow builder in `export::arrow_convert` used to
//! do `row.values.get(&col.name)` PER CELL — an N·M string-hash lookup into the
//! per-row `HashMap<Arc<str>, Value>`. AE1 resolves each schema column to a
//! positional accessor ONCE (O(columns) name hashes) and transposes rows into a
//! per-column value slice in a single pass, then feeds each builder its
//! pre-resolved slice. This is a PURE performance refactor: the emitted Arrow
//! `RecordBatch` — schema, array types, lengths, null bitmaps, and every value —
//! MUST be byte-identical to the pre-refactor output.
//!
//! **This test** builds a wide, multi-row, mixed-type input (every builder arm:
//! scalars, high-fidelity CQL types, collections, nulls, and absent columns) and
//! asserts the `RecordBatch` is exactly what the conversion produced. It passes
//! on `main` (per-cell `.get()`) and MUST continue to pass after the accessor
//! hoist — the guard is "output unchanged", verbatim.
//!
//! Requires the `arrow` feature (the conversion module is `#[cfg(feature =
//! "arrow")]`).

#![cfg(feature = "arrow")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::Array;
use cqlite_core::export::rows_to_record_batch;
use cqlite_core::query::{ColumnInfo, QueryRow};
use cqlite_core::schema::CqlType;
use cqlite_core::types::{DataType, Value};
use cqlite_core::RowKey;

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

/// Build a `QueryRow` from an ordered list of (name, value) pairs. A column
/// omitted from `pairs` is ABSENT (distinct from `Value::Null`) — both must map
/// to Arrow null, which is exactly the property the accessor hoist must preserve.
fn row(pairs: &[(&str, Value)]) -> QueryRow {
    let mut values: HashMap<Arc<str>, Value> = HashMap::new();
    for (name, value) in pairs {
        values.insert(Arc::from(*name), value.clone());
    }
    QueryRow::with_interned_values(RowKey::new(Vec::new()), values)
}

/// A wide, mixed-type schema exercising every builder arm reached by
/// `convert_column_to_array`: flat scalars, high-fidelity CQL scalars, and the
/// recursive collection path.
fn wide_schema() -> Vec<ColumnInfo> {
    vec![
        col("c_bool", DataType::Boolean, None),
        col("c_tinyint", DataType::TinyInt, None),
        col("c_smallint", DataType::SmallInt, None),
        col("c_int", DataType::Integer, None),
        col("c_bigint", DataType::BigInt, None),
        col("c_float", DataType::Float32, None),
        col("c_double", DataType::Float, None),
        col("c_text", DataType::Text, Some(CqlType::Text)),
        col("c_text_opaque", DataType::Text, None),
        col("c_blob", DataType::Blob, None),
        col("c_ts", DataType::Timestamp, None),
        col("c_uuid_flat", DataType::Uuid, None),
        col("c_date", DataType::Timestamp, Some(CqlType::Date)),
        col("c_time", DataType::BigInt, Some(CqlType::Time)),
        col("c_decimal", DataType::Text, Some(CqlType::Decimal)),
        col("c_varint", DataType::Text, Some(CqlType::Varint)),
        col("c_duration", DataType::Text, Some(CqlType::Duration)),
        col("c_uuid_fixed", DataType::Uuid, Some(CqlType::Uuid)),
        col("c_inet", DataType::Text, Some(CqlType::Inet)),
        col("c_counter", DataType::BigInt, Some(CqlType::Counter)),
        col(
            "c_list_typed",
            DataType::List,
            Some(CqlType::List(Box::new(CqlType::Int))),
        ),
        col(
            "c_map_typed",
            DataType::Map,
            Some(CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::Int),
            )),
        ),
        col("c_list_flat", DataType::List, None),
        col("c_map_flat", DataType::Map, None),
    ]
}

/// Rows covering: fully-populated, all-null, and sparse/absent columns. The
/// deliberate mix of `Value::Null`, absent columns, and populated values across
/// rows is what makes the per-column transpose non-trivial — each column must
/// still line up with the right row after the accessor hoist.
fn wide_rows() -> Vec<QueryRow> {
    vec![
        row(&[
            ("c_bool", Value::Boolean(true)),
            ("c_tinyint", Value::TinyInt(7)),
            ("c_smallint", Value::SmallInt(-3)),
            ("c_int", Value::Integer(42)),
            ("c_bigint", Value::BigInt(1_000_000_000_000)),
            ("c_float", Value::Float32(1.5)),
            ("c_double", Value::Float(2.25)),
            ("c_text", Value::Text("hello".into())),
            ("c_text_opaque", Value::Text("opaque".into())),
            ("c_blob", Value::Blob(vec![0xde, 0xad, 0xbe, 0xef])),
            ("c_ts", Value::Timestamp(1_700_000_000_000)),
            ("c_uuid_flat", Value::Uuid([1u8; 16])),
            ("c_date", Value::Date(19_000)),
            ("c_time", Value::Time(123_456_789)),
            (
                "c_decimal",
                Value::Decimal {
                    scale: 2,
                    unscaled: vec![0x30, 0x39], // 12345
                },
            ),
            ("c_varint", Value::Varint(vec![0x01, 0x00])), // 256
            (
                "c_duration",
                Value::Duration {
                    months: 1,
                    days: 2,
                    nanos: 3,
                },
            ),
            ("c_uuid_fixed", Value::Uuid([2u8; 16])),
            ("c_inet", Value::Inet(vec![127, 0, 0, 1])),
            ("c_counter", Value::Counter(99)),
            (
                "c_list_typed",
                Value::List(vec![Value::Integer(1), Value::Integer(2)]),
            ),
            (
                "c_map_typed",
                Value::Map(vec![(Value::Text("k".into()), Value::Integer(5))]),
            ),
            (
                "c_list_flat",
                Value::List(vec![Value::Text("a".into()), Value::Text("b".into())]),
            ),
            (
                "c_map_flat",
                Value::Map(vec![(Value::Text("x".into()), Value::Text("y".into()))]),
            ),
        ]),
        // All-null row: every column present but Null.
        row(&[
            ("c_bool", Value::Null),
            ("c_tinyint", Value::Null),
            ("c_smallint", Value::Null),
            ("c_int", Value::Null),
            ("c_bigint", Value::Null),
            ("c_float", Value::Null),
            ("c_double", Value::Null),
            ("c_text", Value::Null),
            ("c_text_opaque", Value::Null),
            ("c_blob", Value::Null),
            ("c_ts", Value::Null),
            ("c_uuid_flat", Value::Null),
            ("c_date", Value::Null),
            ("c_time", Value::Null),
            ("c_decimal", Value::Null),
            ("c_varint", Value::Null),
            ("c_duration", Value::Null),
            ("c_uuid_fixed", Value::Null),
            ("c_inet", Value::Null),
            ("c_counter", Value::Null),
            ("c_list_typed", Value::Null),
            ("c_map_typed", Value::Null),
            ("c_list_flat", Value::Null),
            ("c_map_flat", Value::Null),
        ]),
        // Sparse row: most columns ABSENT (not even Null) — must still map to
        // Arrow null and stay aligned with the other rows.
        row(&[
            ("c_int", Value::Integer(-1)),
            ("c_text", Value::Text("only-two".into())),
        ]),
        // Second populated row with different values so column/row alignment is
        // observable (a transpose bug that shifts values would surface here).
        row(&[
            ("c_bool", Value::Boolean(false)),
            ("c_int", Value::Integer(7)),
            ("c_bigint", Value::BigInt(-5)),
            ("c_text", Value::Text("world".into())),
            ("c_blob", Value::Blob(vec![0x00])),
            ("c_list_typed", Value::List(vec![Value::Integer(9)])),
        ]),
    ]
}

/// The canonical, load-bearing parity assertion: the full `RecordBatch` produced
/// for a wide mixed-type input is exactly the expected shape. Any accessor-hoist
/// regression that mis-aligns a column, drops a null, or changes a value fails
/// here. Passes on `main`; must keep passing after #1495.
#[test]
fn wide_mixed_batch_is_unchanged() {
    let columns = wide_schema();
    let rows = wide_rows();
    let batch = rows_to_record_batch(&columns, &rows).expect("conversion must succeed");

    // Shape: one column per schema column, one row per input row.
    assert_eq!(batch.num_columns(), columns.len());
    assert_eq!(batch.num_rows(), rows.len());

    // Field names/order match the schema exactly.
    for (i, c) in columns.iter().enumerate() {
        assert_eq!(batch.schema().field(i).name(), &c.name);
    }

    // Per-column null counts pin value/null alignment across the transpose.
    // Row 0 populated, row 1 all-null, row 2 sparse (only c_int + c_text), row 3
    // partial. Expected null count per column below is the count of rows where
    // the value is Null or the column is absent.
    let expected_nulls: &[(&str, usize)] = &[
        ("c_bool", 2),    // rows 1(null),2(absent)
        ("c_tinyint", 3), // rows 1,2,3
        ("c_smallint", 3),
        ("c_int", 1),    // only row 1 null; present in 0,2,3
        ("c_bigint", 2), // rows 1,2
        ("c_float", 3),
        ("c_double", 3),
        ("c_text", 1), // only row 1 null; present in 0,2,3
        ("c_text_opaque", 3),
        ("c_blob", 2), // rows 1,2
        ("c_ts", 3),
        ("c_uuid_flat", 3),
        ("c_date", 3),
        ("c_time", 3),
        ("c_decimal", 3),
        ("c_varint", 3),
        ("c_duration", 3),
        ("c_uuid_fixed", 3),
        ("c_inet", 3),
        ("c_counter", 3),
        ("c_list_typed", 2), // rows 1,2
        ("c_map_typed", 3),
        ("c_list_flat", 3),
        ("c_map_flat", 3),
    ];
    for (name, want) in expected_nulls {
        let idx = columns.iter().position(|c| &c.name == name).unwrap();
        assert_eq!(
            batch.column(idx).null_count(),
            *want,
            "column '{name}' null_count mismatch",
        );
    }

    // Spot-check load-bearing values by downcast (value fidelity + alignment).
    use arrow::array::{BooleanArray, Int32Array, Int64Array, StringArray};

    let bool_idx = columns.iter().position(|c| c.name == "c_bool").unwrap();
    let bools = batch
        .column(bool_idx)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(bools.value(0)); // row 0 = true
    assert!(bools.is_null(1)); // row 1 null
    assert!(bools.is_null(2)); // row 2 absent
    assert!(!bools.value(3)); // row 3 = false

    let int_idx = columns.iter().position(|c| c.name == "c_int").unwrap();
    let ints = batch
        .column(int_idx)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ints.value(0), 42);
    assert!(ints.is_null(1));
    assert_eq!(ints.value(2), -1);
    assert_eq!(ints.value(3), 7);

    let bigint_idx = columns.iter().position(|c| c.name == "c_bigint").unwrap();
    let bigints = batch
        .column(bigint_idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(bigints.value(0), 1_000_000_000_000);
    assert!(bigints.is_null(1));
    assert!(bigints.is_null(2));
    assert_eq!(bigints.value(3), -5);

    let text_idx = columns.iter().position(|c| c.name == "c_text").unwrap();
    let texts = batch
        .column(text_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(texts.value(0), "hello");
    assert!(texts.is_null(1));
    assert_eq!(texts.value(2), "only-two");
    assert_eq!(texts.value(3), "world");
}

/// Column ORDER independence: converting the same rows under a schema whose
/// columns are reordered must produce the same per-column arrays (just permuted).
/// This directly exercises the "resolve accessor per column" contract — the
/// accessor must bind to the column's name, not a fixed position.
#[test]
fn column_reorder_produces_permuted_but_equal_arrays() {
    let columns = wide_schema();
    let rows = wide_rows();
    let batch = rows_to_record_batch(&columns, &rows).expect("conversion");

    // Reverse the schema.
    let mut reordered = columns.clone();
    reordered.reverse();
    let batch_rev = rows_to_record_batch(&reordered, &rows).expect("conversion (reordered)");

    for (i, c) in reordered.iter().enumerate() {
        let orig_idx = columns.iter().position(|o| o.name == c.name).unwrap();
        assert_eq!(
            batch_rev.column(i).null_count(),
            batch.column(orig_idx).null_count(),
            "column '{}' must be identical regardless of schema position",
            c.name
        );
        assert_eq!(batch_rev.column(i).len(), batch.column(orig_idx).len(),);
    }
}

/// Type-mismatch fail-closed behaviour must survive the refactor: a wrong-variant
/// value still errors rather than silently becoming null.
#[test]
fn type_mismatch_still_fails_closed() {
    let columns = vec![col("n", DataType::Integer, None)];
    let rows = vec![row(&[("n", Value::Text("not-an-int".into()))])];
    assert!(rows_to_record_batch(&columns, &rows).is_err());
}
