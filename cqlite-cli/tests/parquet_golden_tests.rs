//! Parquet golden-file test (Issue #685)
//!
//! Guards the CLI `--out parquet` byte stream across the lift of the writer
//! into `cqlite-core` (Epic #682) and any future refactors.  A deterministic
//! fixture `QueryResult` is written through the CLI-facing `ParquetWriter`
//! facade and compared **byte-for-byte** against a checked-in golden file.
//!
//! # Why byte-equality (and when it is expected to break)
//!
//! The Arrow `ArrowWriter` output is fully deterministic for a fixed
//! `arrow`/`parquet` crate version: no timestamps or randomness are embedded,
//! and the `created_by` footer string only changes when the `parquet` crate
//! version changes.  Byte-equality is therefore stable in CI and the strongest
//! possible "output unchanged" guarantee.  On an intentional `arrow`/`parquet`
//! dependency upgrade (or a deliberate format change), regenerate the golden
//! file and review the diff in values via the parsed-equality assertions that
//! run first:
//!
//! ```bash
//! UPDATE_PARQUET_GOLDEN=1 cargo test -p cqlite-cli --test parquet_golden_tests
//! ```

#![cfg(feature = "state_machine")]

use arrow::array::{Array, Int32Array, ListArray, StringArray};
use bytes::Bytes;
use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::ParquetWriter;
use cqlite_core::query::{ColumnInfo, QueryResult, QueryRow};
use cqlite_core::schema::CqlType;
use cqlite_core::types::DataType;
use cqlite_core::{RowKey, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::path::PathBuf;

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/golden_snapshots/parquet_fixture.parquet")
}

/// Deterministic fixture covering scalar, high-fidelity, and collection
/// columns (insertion order is fixed; values are constants).
fn make_fixture() -> QueryResult {
    let mut result = QueryResult::new();
    result.metadata.columns = vec![
        ColumnInfo {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            position: 0,
            table_name: None,
            cql_type: Some(CqlType::Int),
        },
        ColumnInfo {
            name: "name".to_string(),
            data_type: DataType::Text,
            nullable: true,
            position: 1,
            table_name: None,
            cql_type: Some(CqlType::Text),
        },
        ColumnInfo {
            name: "uid".to_string(),
            data_type: DataType::Uuid,
            nullable: true,
            position: 2,
            table_name: None,
            cql_type: Some(CqlType::Uuid),
        },
        ColumnInfo {
            name: "d".to_string(),
            data_type: DataType::Integer,
            nullable: true,
            position: 3,
            table_name: None,
            cql_type: Some(CqlType::Date),
        },
        ColumnInfo {
            name: "tags".to_string(),
            data_type: DataType::List,
            nullable: true,
            position: 4,
            table_name: None,
            cql_type: Some(CqlType::List(Box::new(CqlType::Text))),
        },
        ColumnInfo {
            name: "attrs".to_string(),
            data_type: DataType::Map,
            nullable: true,
            position: 5,
            table_name: None,
            cql_type: Some(CqlType::Map(
                Box::new(CqlType::Text),
                Box::new(CqlType::Int),
            )),
        },
    ];

    for i in 0..3i32 {
        let mut values = HashMap::new();
        values.insert("id".to_string(), Value::Integer(i));
        if i == 2 {
            values.insert("name".to_string(), Value::Null);
        } else {
            values.insert("name".to_string(), Value::Text(format!("row-{i}")));
        }
        values.insert(
            "uid".to_string(),
            Value::Uuid([
                i as u8, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc,
                0xdd, 0xee, 0xff,
            ]),
        );
        values.insert("d".to_string(), Value::Date(19358 + i));
        values.insert(
            "tags".to_string(),
            Value::List(vec![
                Value::Text(format!("a{i}")),
                Value::Text(format!("b{i}")),
            ]),
        );
        values.insert(
            "attrs".to_string(),
            Value::Map(vec![(Value::Text("k".to_string()), Value::Integer(i * 10))]),
        );
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![i as u8]), values));
    }

    result
}

#[test]
fn test_parquet_output_matches_golden_file() {
    let result = make_fixture();
    let bytes =
        ParquetWriter::write(&result, &OutputConfig::default()).expect("parquet write failed");

    // ── parsed sanity assertions (run before byte comparison so a golden
    //    mismatch comes with value-level context) ──
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(&bytes))
        .expect("parquet read-back failed")
        .build()
        .expect("parquet reader build failed");
    let batches: Vec<_> = reader.collect::<Result<_, _>>().expect("batch read failed");
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 6);

    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id should be Int32");
    assert_eq!(ids.values(), &[0, 1, 2]);

    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name should be Utf8");
    assert_eq!(names.value(0), "row-0");
    assert!(names.is_null(2), "row 2 name should be null");

    let tags = batch
        .column(4)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("tags should be List");
    assert_eq!(tags.value_length(0), 2);

    // ── golden byte comparison ──
    let path = golden_path();
    if std::env::var("UPDATE_PARQUET_GOLDEN").is_ok() {
        std::fs::write(&path, &bytes).expect("failed to write golden file");
        eprintln!("golden file regenerated at {}", path.display());
        return;
    }

    let golden = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "failed to read golden file {} ({e}); regenerate with \
             UPDATE_PARQUET_GOLDEN=1 cargo test -p cqlite-cli --test parquet_golden_tests",
            path.display()
        )
    });

    assert_eq!(
        bytes, golden,
        "CLI Parquet output bytes changed; if this is an intentional \
         arrow/parquet upgrade or format change, regenerate with \
         UPDATE_PARQUET_GOLDEN=1 and review the parsed assertions above"
    );
}
