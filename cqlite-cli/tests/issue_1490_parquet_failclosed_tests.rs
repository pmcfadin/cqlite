//! Parquet export fail-closed negative tests — issue #1490 (AD1), epic #1469.
//!
//! # What these assert, and why they are HERE rather than in `cqlite-core`
//!
//! Two fail-closed contracts landed in the CQL→Arrow converter:
//!
//!   * **AC1 / issue #1485** — a `Value` whose runtime type does not match the
//!     column's declared type is an ERROR, never a silently-NULLed cell.
//!   * **AC3 / issue #1487** — a `decimal` whose scale exceeds the fixed Arrow
//!     scale (`Decimal128(38, 9)`) is an ERROR, never a lossy truncation.
//!
//! `cqlite-core` already unit-tests both against `rows_to_record_batch`
//! (`cqlite-core/src/export/arrow_convert_tests.rs`). Those tests pin the
//! CONVERTER. They cannot tell you whether the error still surfaces at the
//! surface an operator actually touches: `cqlite export --format parquet` goes
//! through `cqlite_cli::output::parquet::create_streaming_parquet_writer` →
//! `StreamingWriter::write_chunk`, which is a different code path (streaming,
//! chunked, schema built once at construction) from the batch converter and
//! maps core errors into `OutputError`. A mapping that swallowed the error, or a
//! streaming path that skipped the check, would leave both core unit tests green
//! while the CLI wrote a lossy Parquet file. These tests close that gap: they
//! drive the EXACT constructor `commands/export.rs` calls (see
//! `cqlite-cli/src/commands/export.rs`, `create_streaming_parquet_writer`).
//!
//! Every case pairs the negative with a POSITIVE CONTROL built the same way, so
//! a writer that rejected everything (or a helper that never wrote anything)
//! cannot make the negatives pass vacuously.

#![cfg(feature = "state_machine")]

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;

use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::parquet::create_streaming_parquet_writer_from_writer;
use cqlite_cli::output::{ParquetWriter, StreamingWriter};
use cqlite_core::query::{ColumnInfo, QueryMetadata, QueryResult, QueryRow};
use cqlite_core::schema::CqlType;
use cqlite_core::types::{DataType, Value};
use cqlite_core::RowKey;

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn column(name: &str, data_type: DataType, cql_type: CqlType) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        data_type,
        nullable: true,
        position: 0,
        table_name: None,
        cql_type: Some(cql_type),
    }
}

fn metadata(columns: Vec<ColumnInfo>) -> QueryMetadata {
    QueryMetadata {
        columns,
        ..Default::default()
    }
}

fn row(name: &str, value: Value) -> QueryRow {
    let mut values: HashMap<std::sync::Arc<str>, Value> = HashMap::new();
    values.insert(name.into(), value);
    QueryRow {
        values,
        key: RowKey::new(vec![0]),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

/// Drive the STREAMING writer the `export` subcommand uses, writing into an
/// in-memory buffer, and return the finished Parquet bytes.
///
/// The whole export is one unit: the streaming writer BUFFERS rows and converts
/// them when a row group flushes, so a rejected value surfaces from
/// `write_chunk` or from `finalize` depending on the row-group size. Asserting
/// on a particular call would pin an implementation detail; what the contract
/// says is that the EXPORT fails and says why. The stage is recorded in the
/// message so a diagnostic still names it.
fn stream_rows(columns: Vec<ColumnInfo>, rows: &[QueryRow]) -> Result<Vec<u8>, String> {
    let meta = metadata(columns);
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = create_streaming_parquet_writer_from_writer(&mut buf, &meta, 1_024)
            .map_err(|e| format!("constructor: {e}"))?;
        writer
            .write_chunk(rows)
            .map_err(|e| format!("write_chunk: {e}"))?;
        writer.finalize().map_err(|e| format!("finalize: {e}"))?;
    }
    Ok(buf)
}

/// Drive the BATCH writer the `-e … --out parquet` query path uses.
fn batch_rows(columns: Vec<ColumnInfo>, rows: Vec<QueryRow>) -> Result<Vec<u8>, String> {
    let result = QueryResult {
        rows,
        rows_affected: 0,
        execution_time_ms: 0,
        metadata: metadata(columns),
    };
    ParquetWriter::write(&result, &OutputConfig::default()).map_err(|e| e.to_string())
}

/// Read Parquet bytes back and return (row count, null count of column 0).
///
/// A control that only checked "the writer returned Ok" could not distinguish a
/// real value from the silently-NULLed cell both fail-closed contracts exist to
/// forbid, so every positive control reads its own output back.
fn readback_col0(bytes: &[u8]) -> (usize, usize) {
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
        .expect("control output must be a readable Parquet file")
        .build()
        .expect("control output must build a record batch reader");
    let batches: Vec<RecordBatch> = reader
        .collect::<Result<Vec<_>, _>>()
        .expect("control output must decode");
    let rows = batches.iter().map(|b| b.num_rows()).sum();
    let nulls = batches
        .iter()
        .map(|b| b.column(0).null_count())
        .sum::<usize>();
    (rows, nulls)
}

/// A `decimal` value with the requested scale. `unscaled` is big-endian
/// two's-complement, exactly as the CQL decode path produces it.
fn decimal(scale: i32, unscaled: i64) -> Value {
    Value::Decimal {
        scale,
        unscaled: num_bigint::BigInt::from(unscaled).to_signed_bytes_be(),
    }
}

// ---------------------------------------------------------------------------
// AC1 (#1485) — a mistyped column value must ERROR, not become NULL
// ---------------------------------------------------------------------------

/// A `Text` value in an `int` column: the streaming export the `export`
/// subcommand drives must FAIL, and the diagnostic must name the column.
#[test]
fn ac1_streaming_export_rejects_type_mismatched_value() {
    let err = stream_rows(
        vec![column("age", DataType::Integer, CqlType::Int)],
        &[row("age", Value::Text("not-an-int".into()))],
    )
    .expect_err("a Text value in an int column must fail the Parquet export");
    assert!(
        err.contains("age") && err.contains("expected Int"),
        "the rejection must name the column and the expected type: {err}"
    );
}

/// The same mismatch through the batch writer behind `-e … --out parquet`.
#[test]
fn ac1_batch_export_rejects_type_mismatched_value() {
    let err = batch_rows(
        vec![column("age", DataType::Integer, CqlType::Int)],
        vec![row("age", Value::Text("not-an-int".into()))],
    )
    .expect_err("a Text value in an int column must fail the Parquet export");
    assert!(
        err.contains("age") && err.contains("expected Int"),
        "the rejection must name the column and the expected type: {err}"
    );
}

/// Positive control: the well-typed value exports AND reads back non-NULL.
///
/// Without this, a writer that rejected every row would make the negative above
/// pass for the wrong reason.
#[test]
fn ac1_control_well_typed_value_still_exports() {
    let cols = || vec![column("age", DataType::Integer, CqlType::Int)];

    let bytes = stream_rows(cols(), &[row("age", Value::Integer(41))])
        .expect("a well-typed int must export via the streaming writer");
    assert_eq!(readback_col0(&bytes), (1, 0), "streamed value must be live");
    let arr = int32_col0(&bytes);
    assert_eq!(
        arr,
        vec![Some(41)],
        "streamed value must round-trip exactly"
    );

    let bytes = batch_rows(cols(), vec![row("age", Value::Integer(41))])
        .expect("a well-typed int must export via the batch writer");
    assert_eq!(readback_col0(&bytes), (1, 0));
    assert_eq!(int32_col0(&bytes), vec![Some(41)]);
}

/// Regression guard (#1485): the fail-closed check must not have captured the
/// legitimate NULL path. An explicit `Value::Null` and an ABSENT column both
/// stay NULL — asserted at the CLI surface, where the earlier silent-NULL bug
/// would have been indistinguishable from these.
#[test]
fn ac1_control_null_is_not_a_type_mismatch() {
    let cols = || vec![column("age", DataType::Integer, CqlType::Int)];

    let bytes =
        stream_rows(cols(), &[row("age", Value::Null)]).expect("an explicit NULL must export");
    assert_eq!(readback_col0(&bytes), (1, 1), "explicit NULL stays NULL");

    let absent = QueryRow {
        values: HashMap::new(),
        key: RowKey::new(vec![0]),
        metadata: Default::default(),
        cell_metadata: None,
    };
    let bytes = stream_rows(cols(), &[absent]).expect("an absent column must export as NULL");
    assert_eq!(readback_col0(&bytes), (1, 1), "absent column stays NULL");
}

/// The collection element-dispatch arm: a well-formed `list<int>` whose ELEMENT
/// is mistyped must fail too — the per-element path is a separate arm from the
/// scalar one and had its own silent-NULL fallback.
#[test]
fn ac1_streaming_export_rejects_mistyped_collection_element() {
    let cols = || {
        vec![column(
            "scores",
            DataType::List,
            CqlType::List(Box::new(CqlType::Int)),
        )]
    };
    let err = stream_rows(
        cols(),
        &[row(
            "scores",
            Value::List(vec![Value::Integer(1), Value::Text("bad".into())]),
        )],
    )
    .expect_err("a mistyped list element must fail the Parquet export");
    assert!(
        err.contains("element") && err.contains("expected Int"),
        "the rejection must name the element and the expected type: {err}"
    );

    // Control: the same list with every element well-typed exports.
    let bytes = stream_rows(
        cols(),
        &[row(
            "scores",
            Value::List(vec![Value::Integer(1), Value::Integer(2)]),
        )],
    )
    .expect("a well-typed list must export");
    assert_eq!(readback_col0(&bytes), (1, 0));
}

// ---------------------------------------------------------------------------
// AC3 (#1487) — decimal scale > 9 must ERROR, not truncate
// ---------------------------------------------------------------------------

/// 123456789012 with scale 12 == 0.123456789012 — three fractional digits more
/// than the `Decimal128(38, 9)` target can hold. The pre-#1487 code path scaled
/// this down and succeeded LOSSILY, so the assertion is that the export fails
/// and says it refuses to truncate.
#[test]
fn ac3_streaming_export_rejects_decimal_scale_above_fixed() {
    let err = stream_rows(
        vec![column("amount", DataType::Blob, CqlType::Decimal)],
        &[row("amount", decimal(12, 123_456_789_012))],
    )
    .expect_err("a scale-12 decimal must fail the Parquet export, not truncate");
    assert!(
        err.contains("amount") && err.contains("scale 12") && err.contains("truncate"),
        "the rejection must name the column, the scale and the refusal: {err}"
    );
}

#[test]
fn ac3_batch_export_rejects_decimal_scale_above_fixed() {
    let err = batch_rows(
        vec![column("amount", DataType::Blob, CqlType::Decimal)],
        vec![row("amount", decimal(12, 123_456_789_012))],
    )
    .expect_err("a scale-12 decimal must fail the Parquet export, not truncate");
    assert!(
        err.contains("amount") && err.contains("scale 12") && err.contains("truncate"),
        "the rejection must name the column, the scale and the refusal: {err}"
    );
}

/// Positive control on BOTH sides of the boundary: scale 9 is the largest scale
/// the fixed export scale can represent and must still succeed exactly, and an
/// ordinary scale-3 value must rescale (123.456 → 123_456_000_000 at scale 9).
#[test]
fn ac3_control_decimal_at_or_below_fixed_scale_still_exports() {
    use arrow::array::Decimal128Array;

    for (scale, unscaled, expect) in [
        (3i32, 123_456i64, 123_456_000_000i128),
        (9i32, 123_456i64, 123_456i128),
    ] {
        let bytes = stream_rows(
            vec![column("amount", DataType::Blob, CqlType::Decimal)],
            &[row("amount", decimal(scale, unscaled))],
        )
        .unwrap_or_else(|e| panic!("scale-{scale} decimal must export: {e}"));
        assert_eq!(readback_col0(&bytes), (1, 0), "scale-{scale} must be live");

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .expect("readable")
            .build()
            .expect("reader");
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("decode");
        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128Array");
        assert_eq!(
            arr.value(0),
            expect,
            "scale-{scale} decimal must rescale exactly to the fixed export scale"
        );
    }
}

/// Regression guard (#1487): the scale check must not disturb the NULL path.
#[test]
fn ac3_control_null_decimal_stays_null() {
    let bytes = stream_rows(
        vec![column("amount", DataType::Blob, CqlType::Decimal)],
        &[row("amount", Value::Null)],
    )
    .expect("a NULL decimal must export");
    assert_eq!(readback_col0(&bytes), (1, 1));
}

/// Read column 0 back as `Int32`, so a control asserts the VALUE, not merely
/// that some non-null cell exists.
fn int32_col0(bytes: &[u8]) -> Vec<Option<i32>> {
    use arrow::array::Int32Array;
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.to_vec()))
        .expect("readable Parquet")
        .build()
        .expect("record batch reader");
    let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().expect("decode");
    batches
        .iter()
        .flat_map(|b| {
            let a = b
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Int32Array")
                .clone();
            (0..a.len())
                .map(move |i| if a.is_null(i) { None } else { Some(a.value(i)) })
                .collect::<Vec<_>>()
        })
        .collect()
}
