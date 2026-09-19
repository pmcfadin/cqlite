use super::*;

#[test]
fn test_cql_decimal_rejects_scale_above_fixed_export_scale() {
    let col = col_with_cql_type("v", DataType::Text, cqlite_core::schema::CqlType::Decimal);
    let result = single_cql_typed_result(
        col,
        Value::Decimal {
            scale: 12,
            unscaled: 1_000_000_000_000i64.to_be_bytes().to_vec(),
        },
    );

    let error = ParquetWriter::write(&result, &default_config())
        .expect_err("scale above the fixed export scale must be rejected");
    let error = error
        .downcast_ref::<cqlite_core::export::parquet::ParquetExportError>()
        .expect("decimal rejection must preserve the typed Parquet export error");
    assert!(matches!(
        error,
        cqlite_core::export::parquet::ParquetExportError::InvalidValue(message)
            if message.contains("decimal scale 12")
                && message.contains("refusing to truncate")
    ));
}

#[test]
fn test_cql_decimal_streaming_rejects_scale_above_fixed_export_scale() {
    let col = col_with_cql_type("v", DataType::Text, cqlite_core::schema::CqlType::Decimal);
    let result = single_cql_typed_result(
        col,
        Value::Decimal {
            scale: 12,
            unscaled: 1_000_000_000_000i64.to_be_bytes().to_vec(),
        },
    );
    let options = cqlite_core::export::parquet::ParquetExportOptions {
        row_group_size: 1,
        ..Default::default()
    };
    let mut writer = cqlite_core::export::parquet::StreamingParquetWriter::new(
        Vec::<u8>::new(),
        &result.metadata,
        &options,
    )
    .expect("streaming decimal writer should initialize");

    let error = writer
        .write_chunk(&result.rows)
        .expect_err("streaming scale above the fixed export scale must be rejected");
    assert!(matches!(
        error,
        cqlite_core::export::parquet::ParquetExportError::InvalidValue(message)
            if message.contains("decimal scale 12")
                && message.contains("refusing to truncate")
    ));
}
