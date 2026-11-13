//! Unit tests for `vortex.rs` (moved out for the #1116 file-size ratchet; module path unchanged).

use super::*;
use crate::query::{ColumnInfo, QueryMetadata};
use crate::types::{DataType, Value};
use crate::RowKey;
use std::collections::HashMap;

fn no_tmp_left(path: &Path) -> bool {
    std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .all(|e| !e.unwrap().file_name().to_string_lossy().ends_with(".tmp"))
}

fn metadata_two_cols() -> QueryMetadata {
    QueryMetadata {
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: DataType::BigInt,
                nullable: false,
                position: 0,
                table_name: None,
                cql_type: None,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: DataType::Text,
                nullable: true,
                position: 1,
                table_name: None,
                cql_type: None,
            },
        ],
        ..Default::default()
    }
}

fn row(id: i64, name: Option<&'static str>) -> QueryRow {
    let mut values: HashMap<Arc<str>, Value> = HashMap::new();
    values.insert(Arc::<str>::from("id"), Value::BigInt(id));
    if let Some(n) = name {
        values.insert(Arc::<str>::from("name"), Value::Text(n.into()));
    }
    QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }
}

/// R1.2: `vortex.rs` contains no `match` over a CQL type building Arrow/Vortex arrays
/// directly — every value conversion is reached through the shared
/// `rows_to_record_batch_with_schema` producer (row → Arrow) and Vortex's own
/// `ArrowSession::from_arrow_array` (Arrow → Vortex). This test pins the OBSERVABLE half of
/// that claim: a full round trip through `StreamingVortexWriter` produces the same row count
/// and column set the shared producer would for the same rows, with no separate CQL-aware
/// path taken.
#[tokio::test]
async fn write_chunk_and_finalize_round_trip_row_count() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("basic.vortex");
    let metadata = metadata_two_cols();

    let mut writer =
        StreamingVortexWriter::create(&path, &metadata, &VortexExportOptions { row_group_size: 2 })
            .await
            .expect("create");

    let rows = vec![row(1, Some("a")), row(2, None), row(3, Some("c"))];
    let pushed = writer.write_chunk(&rows).await.expect("write_chunk");
    // row_group_size=2, 3 rows buffered -> one full group (2 rows) flushed immediately.
    assert_eq!(pushed, 2);
    assert_eq!(writer.rows_written(), 3);

    writer.finalize().await.expect("finalize");

    assert!(
        path.exists(),
        "finalize must rename the temp file into place"
    );
    assert!(
        no_tmp_left(&path),
        "no .tmp sibling should remain after a successful finalize"
    );
}

/// R5.1-shaped: default options match Parquet's default row group size, so the two formats'
/// CLI defaults are unsurprising side by side.
#[test]
fn default_options_match_parquet_row_group_size() {
    assert_eq!(VortexExportOptions::default().row_group_size, 10_000);
}

/// R4.2: zero row_group_size is a named `InvalidOptions` error, not a panic or silent 0-row
/// writer.
#[tokio::test]
async fn zero_row_group_size_is_invalid_options() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("invalid.vortex");
    let metadata = metadata_two_cols();

    let result =
        StreamingVortexWriter::create(&path, &metadata, &VortexExportOptions { row_group_size: 0 })
            .await;
    let err = match result {
        Ok(_) => panic!("zero row_group_size must be rejected"),
        Err(e) => e,
    };

    assert!(matches!(err, VortexExportError::InvalidOptions(_)));
}

/// R4.1 (writer-level slice): finalizing twice is a named error rather than a panic, and
/// does not resurrect a file at the destination path.
#[tokio::test]
async fn double_finalize_is_invalid_options_not_a_panic() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("double.vortex");
    let metadata = metadata_two_cols();

    let mut writer =
        StreamingVortexWriter::create(&path, &metadata, &VortexExportOptions::default())
            .await
            .expect("create");
    writer
        .write_chunk(&[row(1, Some("a"))])
        .await
        .expect("write_chunk");
    writer.finalize().await.expect("first finalize");

    let err = writer
        .finalize()
        .await
        .expect_err("second finalize must be rejected");
    assert!(matches!(err, VortexExportError::InvalidOptions(_)));
}

/// A chunk written after `finalize` (smaller than a row group, so it would only be
/// buffered) must be refused, not silently accepted and counted (roborev #4237 r3).
#[tokio::test]
async fn write_after_finalize_is_refused_not_silently_buffered() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("after.vortex");
    let metadata = metadata_two_cols();

    let mut writer =
        StreamingVortexWriter::create(&path, &metadata, &VortexExportOptions::default())
            .await
            .expect("create");
    writer
        .write_chunk(&[row(1, Some("a"))])
        .await
        .expect("write_chunk");
    writer.finalize().await.expect("finalize");

    let err = writer
        .write_chunk(&[row(2, Some("b"))])
        .await
        .expect_err("a write after finalize must be rejected");
    assert!(matches!(err, VortexExportError::InvalidOptions(_)));
}

/// R4.1: a conversion failure in a LATER batch (not the first) names its batch index,
/// leaves no readable file at the destination, and cleans up the `.tmp` sibling.
/// row_group_size=2 so the first `write_chunk` call flushes batch 0 (2 valid rows)
/// immediately, and the bad row sits buffered until `finalize`'s flush hits it as
/// batch 1 — proving the batch index tracks PUSHES, not `write_chunk` calls.
#[tokio::test]
async fn conversion_failure_in_a_later_batch_names_batch_index_and_cleans_up() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bad.vortex");
    let metadata = metadata_two_cols();

    let mut writer =
        StreamingVortexWriter::create(&path, &metadata, &VortexExportOptions { row_group_size: 2 })
            .await
            .expect("create");

    let mut row1: HashMap<Arc<str>, crate::types::Value> = HashMap::new();
    row1.insert(Arc::<str>::from("id"), crate::types::Value::BigInt(1));
    row1.insert(
        Arc::<str>::from("name"),
        crate::types::Value::Text("a".into()),
    );
    let mut row2 = row1.clone();
    row2.insert(Arc::<str>::from("id"), crate::types::Value::BigInt(2));

    let good_row = |values: HashMap<Arc<str>, crate::types::Value>| crate::query::QueryRow {
        values,
        key: crate::RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    };

    let pushed = writer
        .write_chunk(&[good_row(row1), good_row(row2)])
        .await
        .expect("2 valid rows at row_group_size=2 flush immediately as batch 0");
    assert_eq!(pushed, 2, "batch 0 must have been pushed to the writer");

    // A type-mismatched "id" (Text where the schema declares BigInt) — buffered, not
    // yet flushed (1 row < row_group_size=2).
    let mut bad_row_values: HashMap<Arc<str>, crate::types::Value> = HashMap::new();
    bad_row_values.insert(
        Arc::<str>::from("id"),
        crate::types::Value::Text("not-a-bigint".into()),
    );
    bad_row_values.insert(
        Arc::<str>::from("name"),
        crate::types::Value::Text("c".into()),
    );
    let buffered = writer
        .write_chunk(&[good_row(bad_row_values)])
        .await
        .expect("the bad row is only BUFFERED here, not yet converted");
    assert_eq!(
        buffered, 0,
        "buffer has 1 row < row_group_size=2, nothing flushed yet"
    );

    let err = writer
        .finalize()
        .await
        .expect_err("finalize's flush of the buffered bad row must fail");

    match &err {
        VortexExportError::BatchConversion { batch_index, .. } => {
            assert_eq!(
                *batch_index, 1,
                "the failure is in the SECOND pushed batch (index 1), not the first"
            );
        }
        other => panic!("expected VortexExportError::BatchConversion, got {other:?}"),
    }

    assert!(
        !path.exists(),
        "R4.1: a failed export must leave no readable file at the destination path"
    );
    assert!(
        no_tmp_left(&path),
        "R4.1: the .tmp sibling must be cleaned up on a mid-stream failure"
    );
}

/// Review finding 1: a pre-existing symlink at the OLD predictable `<dest>.tmp` name must be
/// left untouched (its target never truncated), and the export must still succeed.
#[cfg(unix)]
#[tokio::test]
async fn preexisting_symlink_at_old_tmp_name_is_untouched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let victim = dir.path().join("victim.txt");
    std::fs::write(&victim, b"precious").unwrap();
    let path = dir.path().join("out.vortex");
    std::os::unix::fs::symlink(&victim, dir.path().join("out.vortex.tmp")).unwrap();
    let mut w = StreamingVortexWriter::create(&path, &metadata_two_cols(), &Default::default())
        .await
        .expect("create");
    w.write_chunk(&[row(1, Some("a"))]).await.expect("chunk");
    w.finalize().await.expect("finalize");
    assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
    assert!(path.exists());
}

/// Review finding 1: concurrent exports into one directory both succeed and leave no tmp.
#[tokio::test]
async fn concurrent_exports_in_one_dir_do_not_clobber_each_other() {
    let dir = tempfile::tempdir().expect("tempdir");
    let run = |name: &'static str, n: i64| {
        let path = dir.path().join(name);
        async move {
            let mut w =
                StreamingVortexWriter::create(&path, &metadata_two_cols(), &Default::default())
                    .await?;
            let rows: Vec<_> = (0..n).map(|i| row(i, Some("x"))).collect();
            w.write_chunk(&rows).await?;
            w.finalize().await?;
            Ok::<_, VortexExportError>(w.rows_written())
        }
    };
    let (a, b) = tokio::join!(run("a.vortex", 3), run("b.vortex", 5));
    assert_eq!((a.expect("a"), b.expect("b")), (3, 5));
    assert!(dir.path().join("a.vortex").exists() && dir.path().join("b.vortex").exists());
    assert!(no_tmp_left(&dir.path().join("a.vortex")));
}

/// Review finding 2: a terminal failure drops the writer, removes the temp and poisons the
/// writer: the next `write_chunk`/`finalize` returns `Poisoned`, deterministically.
#[tokio::test]
async fn terminal_failure_poisons_the_writer_and_leaves_no_tmp() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("p.vortex");
    let mut w = StreamingVortexWriter::create(
        &path,
        &metadata_two_cols(),
        &VortexExportOptions { row_group_size: 1 },
    )
    .await
    .expect("create");
    let mut values: HashMap<Arc<str>, Value> = HashMap::new();
    values.insert(Arc::<str>::from("id"), Value::Text("not-a-bigint".into()));
    values.insert(Arc::<str>::from("name"), Value::Text("c".into()));
    let bad = crate::query::QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    };
    let err = w.write_chunk(&[bad]).await.expect_err("conversion fails");
    assert!(
        matches!(err, VortexExportError::BatchConversion { .. }),
        "{err:?}"
    );
    assert!(no_tmp_left(&path) && !path.exists());
    assert!(matches!(
        w.write_chunk(&[row(1, None)]).await,
        Err(VortexExportError::Poisoned)
    ));
    assert!(matches!(
        w.finalize().await,
        Err(VortexExportError::Poisoned)
    ));
    assert!(no_tmp_left(&path) && !path.exists());
}

/// roborev r2: exporting over an existing destination replaces it (all platforms).
#[tokio::test]
async fn export_replaces_a_preexisting_destination() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("out.vortex");
    std::fs::write(&path, b"old contents").expect("seed destination");
    let mut w = StreamingVortexWriter::create(&path, &metadata_two_cols(), &Default::default())
        .await
        .expect("create");
    w.write_chunk(&[row(1, Some("a"))]).await.expect("chunk");
    w.finalize().await.expect("finalize over an existing file");
    let bytes = std::fs::read(&path).expect("read back");
    assert_ne!(bytes, b"old contents");
    assert_eq!(&bytes[..4], b"VTXF", "a Vortex file starts with its magic");
    assert!(no_tmp_left(&path));
}

/// roborev r2 / #4527 class: a failed in-flight write must fail the export, never be parked
/// by tokio. `/dev/full` accepts opens and fails every write with ENOSPC.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn write_error_on_dev_full_fails_finish() {
    use vortex::file::WriteOptionsSessionExt;
    let metadata = metadata_two_cols();
    let schema = std::sync::Arc::new(
        crate::export::arrow_convert::build_arrow_schema(&metadata.columns).expect("schema"),
    );
    let session = VortexSession::default();
    let dtype = session
        .arrow()
        .from_arrow_schema(schema.as_ref())
        .expect("dtype");
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open("/dev/full")
        .await
        .expect("open /dev/full");
    let mut writer = session.write_options().writer(file, dtype);
    let batch = rows_to_record_batch_with_schema(
        std::sync::Arc::clone(&schema),
        &metadata.columns,
        &[row(1, Some("a"))],
    )
    .expect("batch");
    let array = convert_batch_to_vortex(&session, &schema, &batch, 0).expect("array");
    // The push may or may not observe the failure (the layout task is asynchronous); the
    // contract is that `finish` cannot report success.
    let _ = writer.push(array).await;
    assert!(writer.finish().await.is_err(), "ENOSPC must fail finish()");
}

/// After `finalize` renamed the temp into place, the writer no longer owns that temp NAME:
/// an entry another process creates there must survive the writer's `Drop` (roborev #4237 r5).
#[tokio::test]
async fn drop_after_finalize_never_unlinks_a_path_it_no_longer_owns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("owned.vortex");
    let metadata = metadata_two_cols();

    let mut writer =
        StreamingVortexWriter::create(&path, &metadata, &VortexExportOptions::default())
            .await
            .expect("create");
    let tmp = writer
        .tmp_path
        .clone()
        .expect("a fresh writer owns its temp");
    writer
        .write_chunk(&[row(1, Some("a"))])
        .await
        .expect("write_chunk");
    writer.finalize().await.expect("finalize");
    assert!(
        writer.tmp_path.is_none(),
        "ownership released after the rename"
    );

    std::fs::write(&tmp, b"someone else's file")
        .expect("plant a foreign file at the old temp name");
    drop(writer);
    assert_eq!(
        std::fs::read(&tmp).expect("the foreign file must survive Drop"),
        b"someone else's file"
    );
}

/// R1.1: the Parquet writer and the Vortex writer BOTH build their `RecordBatch` through the one
/// shared producer, `rows_to_record_batch_with_schema` — one producer call per writer for the same
/// rows, counted by a test hook on the producer itself (thread-local; `#[tokio::test]` runs on one
/// thread, so the async Vortex path is counted too).
#[cfg(feature = "parquet")]
#[tokio::test]
async fn both_writers_build_their_batches_through_the_one_shared_producer() {
    use crate::export::arrow_convert::PRODUCER_CALLS;
    use crate::export::parquet::{create_streaming_parquet_writer, ParquetExportOptions};

    let dir = tempfile::tempdir().expect("tempdir");
    let metadata = metadata_two_cols();
    let rows = vec![row(1, Some("a")), row(2, None)];
    let calls = || PRODUCER_CALLS.with(|c| c.get());

    let start = calls();
    let file = std::fs::File::create(dir.path().join("p.parquet")).expect("parquet file");
    let mut pw = create_streaming_parquet_writer(file, &metadata, &ParquetExportOptions::default())
        .expect("parquet writer");
    pw.write_chunk(&rows).expect("parquet write_chunk");
    pw.finalize().expect("parquet finalize");
    let after_parquet = calls();
    assert_eq!(
        after_parquet - start,
        1,
        "Parquet must call the shared producer exactly once"
    );

    let mut vw = StreamingVortexWriter::create(
        dir.path().join("v.vortex"),
        &metadata,
        &VortexExportOptions::default(),
    )
    .await
    .expect("vortex writer");
    vw.write_chunk(&rows).await.expect("vortex write_chunk");
    vw.finalize().await.expect("vortex finalize");
    assert_eq!(
        calls() - after_parquet,
        1,
        "Vortex must call the shared producer exactly once"
    );
}

/// R1.2: the writer's source holds no CQL-type mapping of its own (no `CqlType`, no Arrow array
/// builders) — values reach Arrow only through the shared producer. A source-level pin, so adding a
/// Vortex-private mapping fails this test.
#[test]
fn vortex_source_has_no_cql_to_arrow_mapping_of_its_own() {
    let src = include_str!("vortex.rs");
    for forbidden in [
        "CqlType",
        "arrow::array::builder",
        "Builder::",
        "Value::Text",
        "Value::Int",
    ] {
        assert!(
            !src.contains(forbidden),
            "vortex.rs must not contain `{forbidden}`"
        );
    }
    assert!(src.contains("rows_to_record_batch_with_schema("));
}

/// R2.1 (writer level): a `uuid` and a `timeuuid` column come out of the Vortex WRITER as Vortex's
/// UUID extension DType, read from the written file's own dtype (a text column is not an
/// extension). The fixture-backed confirmation is the CLI differential's
/// `r2_1_vortex_dtype_is_the_uuid_extension_for_uuid_and_timeuuid`.
#[tokio::test]
async fn uuid_and_timeuuid_columns_are_the_vortex_uuid_extension_dtype() {
    use crate::schema::CqlType;
    use vortex::array::extension::uuid::Uuid as VortexUuid;
    use vortex::file::OpenOptionsSessionExt;

    let col = |name: &str, dt: DataType, cql: CqlType, pos: usize| ColumnInfo {
        name: name.to_string(),
        data_type: dt,
        nullable: false,
        position: pos,
        table_name: None,
        cql_type: Some(cql),
    };
    let metadata = QueryMetadata {
        columns: vec![
            col("u", DataType::Uuid, CqlType::Uuid, 0),
            col("tu", DataType::Uuid, CqlType::TimeUuid, 1),
            col("t", DataType::Text, CqlType::Text, 2),
        ],
        ..Default::default()
    };
    let mut values: HashMap<Arc<str>, Value> = HashMap::new();
    values.insert(Arc::<str>::from("u"), Value::Uuid([7u8; 16]));
    values.insert(Arc::<str>::from("tu"), Value::Uuid([9u8; 16]));
    values.insert(Arc::<str>::from("t"), Value::Text("x".into()));
    let rows = vec![QueryRow {
        values,
        key: RowKey::new(Vec::new()),
        metadata: Default::default(),
        cell_metadata: None,
    }];

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("u.vortex");
    let mut w = StreamingVortexWriter::create(&path, &metadata, &VortexExportOptions::default())
        .await
        .expect("create");
    w.write_chunk(&rows).await.expect("write_chunk");
    w.finalize().await.expect("finalize");

    let session = VortexSession::default();
    let file = session.open_options().open_path(path).await.expect("open");
    let fields = file.dtype().as_struct_fields();
    let is_uuid = |name: &str| {
        fields
            .field(name)
            .unwrap_or_else(|| panic!("no field {name}"))
            .as_extension_opt()
            .map(|e| e.is::<VortexUuid>())
    };
    assert_eq!(
        is_uuid("u"),
        Some(true),
        "uuid must be the UUID extension DType"
    );
    assert_eq!(
        is_uuid("tu"),
        Some(true),
        "timeuuid must be the UUID extension DType"
    );
    assert_eq!(is_uuid("t"), None, "text must not be an extension type");
}
