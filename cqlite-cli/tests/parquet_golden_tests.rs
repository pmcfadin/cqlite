//! Parquet golden-file test (Issue #685)
//!
//! Guards the CLI `--out parquet` output across the lift of the writer into
//! `cqlite-core` (Epic #682) and dependency upgrades. The retained Cassandra
//! fixture is compared at the semantic Parquet boundary: decoded values,
//! Arrow/Parquet schema, and stable row-group properties. Page headers and
//! footer/index serialization may change between compatible writer versions.

#![cfg(feature = "state_machine")]

use arrow::array::Array;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use cqlite_cli::config::OutputConfig;
use cqlite_cli::output::{
    create_streaming_parquet_writer_from_writer, ParquetWriter, StreamingWriter,
};
use cqlite_core::query::{ColumnInfo, QueryResult, QueryRow};
use cqlite_core::schema::CqlType;
use cqlite_core::types::DataType;
use cqlite_core::{RowKey, Value};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::column::page::PageReader;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{ReaderProperties, WriterProperties};
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::file::statistics::Statistics;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

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
            values.insert("name".to_string(), Value::text(format!("row-{i}")));
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
                Value::text(format!("a{i}")),
                Value::text(format!("b{i}")),
            ]),
        );
        values.insert(
            "attrs".to_string(),
            Value::Map(vec![(Value::text("k".to_string()), Value::Integer(i * 10))]),
        );
        result
            .rows
            .push(QueryRow::with_values(RowKey::new(vec![i as u8]), values));
    }

    result
}

fn read_parquet_snapshot(bytes: &[u8]) -> (Arc<ParquetMetaData>, SchemaRef, Vec<RecordBatch>) {
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::copy_from_slice(bytes))
        .expect("parquet read-back failed");
    let metadata = builder.metadata().clone();
    let schema = builder.schema().clone();
    let batches = builder
        .build()
        .expect("parquet reader build failed")
        .collect::<Result<Vec<_>, _>>()
        .expect("batch read failed");
    (metadata, schema, batches)
}

fn statistics_match(actual: Option<&Statistics>, expected: Option<&Statistics>) -> bool {
    match (actual, expected) {
        (None, None) => true,
        (Some(actual), Some(expected)) => {
            (
                actual.physical_type(),
                actual.min_bytes_opt(),
                actual.max_bytes_opt(),
                actual.null_count_opt(),
                actual.distinct_count_opt(),
                actual.min_is_exact(),
                actual.max_is_exact(),
                actual.is_min_max_deprecated(),
            ) == (
                expected.physical_type(),
                expected.min_bytes_opt(),
                expected.max_bytes_opt(),
                expected.null_count_opt(),
                expected.distinct_count_opt(),
                expected.min_is_exact(),
                expected.max_is_exact(),
                expected.is_min_max_deprecated(),
            )
        }
        _ => false,
    }
}

/// Compare stable Parquet semantics. Offsets, encoded sizes, page-header
/// bytes, and the `created_by` writer marker may vary across compatible
/// parquet-rs versions and are intentionally excluded. The Arrow59 writer
/// also emits per-page `encoding_stats` that the retained Arrow53 file lacks;
/// that one footer field is an intentional serialization difference. The
/// file-level column orders and row-group sorting columns remain part of this
/// oracle so metadata is not broadly ignored.
fn parquet_semantics_match(actual: &[u8], expected: &[u8]) -> bool {
    let (actual_metadata, actual_schema, actual_batches) = read_parquet_snapshot(actual);
    let (expected_metadata, expected_schema, expected_batches) = read_parquet_snapshot(expected);

    if (
        actual_schema.as_ref(),
        actual_metadata.file_metadata().key_value_metadata(),
        actual_metadata.file_metadata().schema_descr(),
        actual_metadata.file_metadata().version(),
        actual_metadata.file_metadata().num_rows(),
        actual_metadata.file_metadata().column_orders(),
        actual_metadata.num_row_groups(),
    ) != (
        expected_schema.as_ref(),
        expected_metadata.file_metadata().key_value_metadata(),
        expected_metadata.file_metadata().schema_descr(),
        expected_metadata.file_metadata().version(),
        expected_metadata.file_metadata().num_rows(),
        expected_metadata.file_metadata().column_orders(),
        expected_metadata.num_row_groups(),
    ) {
        return false;
    }

    let actual_batch = concat_batches(&actual_schema, &actual_batches)
        .expect("failed to concatenate actual Parquet batches");
    let expected_batch = concat_batches(&expected_schema, &expected_batches)
        .expect("failed to concatenate golden Parquet batches");
    if (actual_batch.num_rows(), actual_batch.num_columns())
        != (expected_batch.num_rows(), expected_batch.num_columns())
    {
        return false;
    }
    for (actual, expected) in actual_batch.columns().iter().zip(expected_batch.columns()) {
        if actual.to_data() != expected.to_data() {
            return false;
        }
    }

    for (actual, expected) in actual_metadata
        .row_groups()
        .iter()
        .zip(expected_metadata.row_groups())
    {
        if (
            actual.num_rows(),
            actual.num_columns(),
            actual.sorting_columns(),
        ) != (
            expected.num_rows(),
            expected.num_columns(),
            expected.sorting_columns(),
        ) {
            return false;
        }
        for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
            if (
                actual.column_descr(),
                actual.num_values(),
                actual.compression_codec(),
                actual.encodings().collect::<Vec<_>>(),
            ) != (
                expected.column_descr(),
                expected.num_values(),
                expected.compression_codec(),
                expected.encodings().collect::<Vec<_>>(),
            ) {
                return false;
            }
            if !statistics_match(actual.statistics(), expected.statistics()) {
                return false;
            }
        }
    }
    true
}

#[test]
fn test_parquet_output_matches_golden_semantics() {
    let result = make_fixture();
    let bytes =
        ParquetWriter::write(&result, &OutputConfig::default()).expect("parquet write failed");
    let path = golden_path();
    let golden = std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "failed to read retained golden file {} ({e})",
            path.display()
        )
    });
    assert!(
        parquet_semantics_match(&bytes, &golden),
        "CLI Parquet output differs from the retained golden semantically"
    );
}

fn assert_semantic_mismatch_is_rejected(actual: &[u8], golden: &[u8], kind: &str) {
    assert!(
        !parquet_semantics_match(actual, golden),
        "semantic comparator accepted a {kind} mismatch"
    );
}

#[test]
fn test_parquet_golden_rejects_value_schema_and_property_mismatches() {
    let golden = std::fs::read(golden_path()).expect("retained golden fixture missing");

    let mut value_changed = make_fixture();
    value_changed.rows[0]
        .values
        .insert("id".into(), Value::Integer(99));
    let value_bytes = ParquetWriter::write(&value_changed, &OutputConfig::default())
        .expect("changed-value fixture should write");
    assert_semantic_mismatch_is_rejected(&value_bytes, &golden, "value");

    let mut schema_changed = make_fixture();
    schema_changed.metadata.columns[2].nullable = false;
    let schema_bytes = ParquetWriter::write(&schema_changed, &OutputConfig::default())
        .expect("changed-schema fixture should write");
    assert_semantic_mismatch_is_rejected(&schema_bytes, &golden, "schema");

    let property_fixture = make_fixture();
    let property_batch = cqlite_core::export::rows_to_record_batch(
        &property_fixture.metadata.columns,
        &property_fixture.rows,
    )
    .expect("property fixture conversion should succeed");
    let mut property_bytes = Vec::new();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer = ArrowWriter::try_new(
        &mut property_bytes,
        property_batch.schema(),
        Some(properties),
    )
    .expect("property fixture writer should initialize");
    writer
        .write(&property_batch)
        .expect("property fixture should write");
    writer.close().expect("property fixture should close");
    assert_semantic_mismatch_is_rejected(&property_bytes, &golden, "compression");
}

fn long_text_fixture() -> QueryResult {
    let mut result = QueryResult::new();
    result.metadata.columns = vec![ColumnInfo {
        name: "text".to_string(),
        data_type: DataType::Text,
        nullable: false,
        position: 0,
        table_name: None,
        cql_type: Some(CqlType::Text),
    }];
    for (index, value) in ["a".repeat(96), "z".repeat(96)].into_iter().enumerate() {
        let mut values = HashMap::new();
        values.insert("text".to_string(), Value::text(value));
        result.rows.push(QueryRow::with_values(
            RowKey::new(vec![index as u8]),
            values,
        ));
    }
    result
}

fn assert_long_text_statistics(bytes: &[u8], min: &str, max: &str) {
    let (metadata, _, _) = read_parquet_snapshot(bytes);
    let statistics = metadata
        .row_group(0)
        .column(0)
        .statistics()
        .expect("long text column statistics should be present");
    assert_eq!(statistics.min_bytes_opt(), Some(min.as_bytes()));
    assert_eq!(statistics.max_bytes_opt(), Some(max.as_bytes()));
}

fn assert_long_text_page_statistics(bytes: &[u8], min: &str, max: &str) {
    let (metadata, _, _) = read_parquet_snapshot(bytes);
    let row_group = metadata.row_group(0);
    let column = row_group.column(0);
    let reader_properties = Arc::new(
        ReaderProperties::builder()
            .set_read_page_statistics(true)
            .build(),
    );
    let mut page_reader = SerializedPageReader::new_with_properties(
        Arc::new(Bytes::copy_from_slice(bytes)),
        column,
        row_group.num_rows() as usize,
        None,
        reader_properties,
    )
    .expect("long text page reader should initialize");

    let mut data_pages = 0;
    while let Some(page) = page_reader
        .get_next_page()
        .expect("long text page should decode")
    {
        if !page.is_data_page() {
            continue;
        }
        data_pages += 1;
        let statistics = page
            .statistics()
            .expect("long text data page statistics should be present");
        assert_eq!(statistics.min_bytes_opt(), Some(min.as_bytes()));
        assert_eq!(statistics.max_bytes_opt(), Some(max.as_bytes()));
    }
    assert!(
        data_pages > 0,
        "long text fixture should contain a data page"
    );
}

#[test]
fn test_parquet_long_text_statistics_are_not_truncated() {
    let result = long_text_fixture();
    let min = "a".repeat(96);
    let max = "z".repeat(96);

    let batch_bytes = ParquetWriter::write(&result, &OutputConfig::default())
        .expect("batch long-text fixture should write");
    assert_long_text_statistics(&batch_bytes, &min, &max);
    assert_long_text_page_statistics(&batch_bytes, &min, &max);

    let tmp = tempfile::NamedTempFile::new().expect("temporary Parquet path");
    let file = std::fs::File::create(tmp.path()).expect("temporary Parquet file");
    let mut writer = create_streaming_parquet_writer_from_writer(file, &result.metadata, 100)
        .expect("streaming long-text writer should initialize");
    writer
        .write_chunk(&result.rows)
        .expect("streaming long-text fixture should write");
    writer
        .finalize()
        .expect("streaming long-text writer should close");
    drop(writer);
    let streaming_bytes = std::fs::read(tmp.path()).expect("streaming Parquet output");
    assert_long_text_statistics(&streaming_bytes, &min, &max);
    assert_long_text_page_statistics(&streaming_bytes, &min, &max);
}
