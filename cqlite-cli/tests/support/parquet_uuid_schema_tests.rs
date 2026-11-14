//! Nested-UUID Arrow schema tests (#4237: element/key fields carry `arrow.uuid` metadata).
//! Split out of `parquet_writer_tests.rs` for the #1135 file-size ratchet; names unchanged.

use super::*;

#[test]
fn test_set_uuid_schema_is_list_fixedsizebinary16() {
    use arrow::datatypes::DataType as ArrowDataType;

    let col = set_col_with_cql_type(
        "uuids",
        cqlite_core::schema::CqlType::Set(Box::new(cqlite_core::schema::CqlType::Uuid)),
    );
    let uuid1 = [0x01u8; 16];
    let result = single_cql_typed_result(col, Value::Set(vec![Value::Uuid(uuid1)]));

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let schema = batch.schema();
    let field = schema.field(0);
    // #4237: list element carries `arrow.uuid` metadata like a top-level uuid column.
    let mut uuid_meta = std::collections::HashMap::new();
    uuid_meta.insert("ARROW:extension:name".to_string(), "arrow.uuid".to_string());
    assert_eq!(
        field.data_type(),
        &ArrowDataType::List(std::sync::Arc::new(
            arrow::datatypes::Field::new("item", ArrowDataType::FixedSizeBinary(16), true)
                .with_metadata(uuid_meta)
        ))
    );
}

#[test]
fn test_map_uuid_timestamp_schema() {
    use arrow::datatypes::{DataType as ArrowDataType, Fields, TimeUnit};
    use std::sync::Arc;

    let col = map_col_with_cql_type(
        "events",
        cqlite_core::schema::CqlType::Map(
            Box::new(cqlite_core::schema::CqlType::Uuid),
            Box::new(cqlite_core::schema::CqlType::Timestamp),
        ),
    );
    let result = single_cql_typed_result(
        col,
        Value::Map(vec![(
            Value::Uuid([0xABu8; 16]),
            Value::Timestamp(1_000_000_000),
        )]),
    );

    let bytes = ParquetWriter::write(&result, &default_config()).unwrap();
    let batch = read_parquet_back(&bytes).unwrap();

    let schema = batch.schema();
    let field = schema.field(0);

    // #4237: map key carries `arrow.uuid` metadata like a top-level uuid column.
    let mut uuid_meta = std::collections::HashMap::new();
    uuid_meta.insert("ARROW:extension:name".to_string(), "arrow.uuid".to_string());
    let expected_entries = Arc::new(arrow::datatypes::Field::new(
        "entries",
        ArrowDataType::Struct(Fields::from(vec![
            arrow::datatypes::Field::new("key", ArrowDataType::FixedSizeBinary(16), false)
                .with_metadata(uuid_meta),
            arrow::datatypes::Field::new(
                "value",
                ArrowDataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
        ])),
        false,
    ));
    assert_eq!(
        field.data_type(),
        &ArrowDataType::Map(expected_entries, false)
    );
}
