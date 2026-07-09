//! Emit the deterministic goldens the Trino connector decodes against, produced
//! by the **REAL** cqlite-flight server emission path — never a hand-built blob.
//!
//! Two goldens, both from the same `MergeProducer` + wire
//! [`arrow_schema`](MergeProducer::arrow_schema) the server's `do_get` uses:
//!
//! 1. **`all_scalars.arrows`** (issue #2234) — an Arrow IPC **stream** (via
//!    [`StreamWriter`]) covering every scalar CQL type, decoded by
//!    `ArrowToTrinoGoldenTest`. The wire schema carries the uuid extension
//!    metadata, `Timestamp(Millisecond, "UTC")` unit, `Date32`, and the
//!    `cqlite:pushdown` field metadata.
//!
//! 2. **`keyvalue.flightdata`** (issue #2193) — the protobuf-encoded
//!    **`FlightData` message sequence** for the exact FIELD failure shape (a
//!    3-row `cassandra_easy_stress.keyvalue`: `key text, value text`, 1 pk, 0 ck,
//!    with the `cqlite:pushdown` field metadata), produced by the SAME
//!    [`FlightDataEncoderBuilder`] path as production
//!    (`streaming.rs::encode_do_get`). This is Flight's on-the-wire framing (a
//!    bare IPC `Message` flatbuffer in each `data_header` + a separate
//!    `data_body`), which is a DIFFERENT framing from the IPC stream in (1) —
//!    `FlightDataGoldenDecodeTest` decodes it with arrow-java's Flight-level
//!    machinery to catch cross-stack Flight interop failures that the
//!    `ArrowStreamReader` golden cannot see. Length-delimited (each message
//!    prefixed by a protobuf varint length) so Java can split the sequence with
//!    `Flight.FlightData.parseDelimitedFrom`.
//!
//! Regenerate BOTH with `trino-connector/scripts/regen-arrow-golden.sh` — do NOT
//! hand-edit either blob. Run:
//! `cargo run -p cqlite-flight --example emit_arrow_golden -- <arrows-out> [<flightdata-out>]`.

use std::collections::HashMap;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::FlightData;
use futures::TryStreamExt;
use prost::Message as _;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;

use cqlite_flight::producer::{DirSource, MergeProducer};
// The field-shape `keyvalue` fixture (schema/rows/mutations/batch size) is the
// single source of truth shared with `tests/do_get_transport_test.rs` so the
// golden this example emits and the wire bytes that test byte-compares against it
// can never drift apart (issue #2283).
use cqlite_flight::test_fixtures as fx;

const KS: &str = "golden_ks";
const TBL: &str = "all_scalars";

/// A partition-keyed table with one column of every supported scalar CQL type.
/// PK is `id int`; the rest are regular columns so a single mutation can write a
/// value for each and exercise every Arrow array builder.
fn schema() -> TableSchema {
    let col = |name: &str, ty: &str, nullable: bool| Column {
        name: name.into(),
        data_type: ty.into(),
        nullable,
        default: None,
        is_static: false,
    };
    TableSchema {
        keyspace: KS.into(),
        table: TBL.into(),
        partition_keys: vec![KeyColumn {
            name: "id".into(),
            data_type: "int".into(),
            position: 0,
        }],
        clustering_keys: vec![],
        columns: vec![
            col("id", "int", false),
            col("c_bool", "boolean", true),
            col("c_tinyint", "tinyint", true),
            col("c_smallint", "smallint", true),
            col("c_bigint", "bigint", true),
            col("c_float", "float", true),
            col("c_double", "double", true),
            col("c_text", "text", true),
            col("c_blob", "blob", true),
            col("c_timestamp", "timestamp", true),
            col("c_date", "date", true),
            col("c_time", "time", true),
            col("c_uuid", "uuid", true),
        ],
        comments: HashMap::new(),
        dropped_columns: HashMap::new(),
    }
}

/// The single fixture UUID, hard-pinned so the golden is byte-deterministic and
/// the Java assertion can hard-code the canonical hyphenated form.
const FIXTURE_UUID: [u8; 16] = [
    0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0x4d, 0xef, 0x81, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
];

/// The single fixture CQL `time` value, hard-pinned as nanoseconds-of-day so the
/// golden's `Time64(Nanosecond)` column is byte-deterministic and the Java
/// assertion can hard-code the exact value. This is `13:14:15.123456789`
/// = ((13*3600 + 14*60 + 15) * 1_000_000_000) + 123_456_789.
const FIXTURE_TIME_NANOS: i64 = 47_655_123_456_789;

/// One full row (`id = 1`) with a value for every scalar column, plus a sparse
/// row (`id = 2`) that writes only `c_text` so every other non-key column is
/// null — so the golden also pins the server's null encoding for each Arrow type.
fn mutations() -> Vec<Mutation> {
    let full = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(1)),
        None,
        vec![
            CellOperation::Write {
                column: "c_bool".into(),
                value: Value::Boolean(true),
            },
            CellOperation::Write {
                column: "c_tinyint".into(),
                value: Value::TinyInt(-7),
            },
            CellOperation::Write {
                column: "c_smallint".into(),
                value: Value::SmallInt(1234),
            },
            CellOperation::Write {
                column: "c_bigint".into(),
                value: Value::BigInt(9_876_543_210),
            },
            CellOperation::Write {
                column: "c_float".into(),
                value: Value::Float32(2.5),
            },
            CellOperation::Write {
                column: "c_double".into(),
                value: Value::Float(6.25),
            },
            CellOperation::Write {
                column: "c_text".into(),
                value: Value::Text("héllo".into()),
            },
            CellOperation::Write {
                column: "c_blob".into(),
                value: Value::Blob(vec![0xde, 0xad, 0xbe, 0xef]),
            },
            CellOperation::Write {
                column: "c_timestamp".into(),
                value: Value::Timestamp(1_700_000_000_000),
            },
            CellOperation::Write {
                column: "c_date".into(),
                value: Value::Date(19_000),
            },
            // 13:14:15.123456789 as nanoseconds-of-day — a fixed, deterministic
            // time-of-day (NOT wall-clock), pinned so the Java assertion can
            // hard-code the exact nanosecond value.
            CellOperation::Write {
                column: "c_time".into(),
                value: Value::Time(FIXTURE_TIME_NANOS),
            },
            CellOperation::Write {
                column: "c_uuid".into(),
                value: Value::Uuid(FIXTURE_UUID),
            },
        ],
        100,
        None,
    );
    // Sparse row: writes ONLY `c_text` so every OTHER regular column (bool,
    // the integer family, float/double, blob, timestamp, date, time, uuid) is null
    // in the merged output — pinning the server's null encoding for each Arrow type.
    // A live row needs at least one written cell (there is no bare row marker in
    // this mutation API), hence one non-null column here.
    let nulls = Mutation::new(
        TableId::new(KS, TBL),
        PartitionKey::single("id", Value::Integer(2)),
        None,
        vec![CellOperation::Write {
            column: "c_text".into(),
            value: Value::Text("only-text".into()),
        }],
        100,
        None,
    );
    vec![full, nulls]
}

/// Flush `mutations` into a fresh single-SSTable write-engine fixture under a
/// temp dir; return the temp handle (kept alive by the caller) and the data dir.
fn build_fixture(
    schema: &TableSchema,
    mutations: Vec<Mutation>,
) -> Result<(tempfile::TempDir, PathBuf), Box<dyn std::error::Error>> {
    let temp = tempfile::TempDir::new()?;
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config)?;
    for m in mutations {
        engine.write(m)?;
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(engine.flush())?
        .ok_or("flush produced no SSTable")?;
    Ok((temp, data_dir))
}

/// Emit the `all_scalars` Arrow IPC **stream** golden (issue #2234). Serialized
/// with the server's WIRE schema so the golden carries the exact field metadata
/// (uuid extension, Timestamp unit, `cqlite:pushdown`) the connector receives.
fn emit_arrows_golden(out: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = schema();
    let (_temp, data_dir) = build_fixture(&schema, mutations())?;

    let producer = MergeProducer::new(schema, 1024)?;
    let wire_schema = Arc::new(producer.arrow_schema()?);
    let table_dir = data_dir.join(KS).join(TBL);
    let batches = producer.produce(&DirSource::new(table_dir))?;

    if batches.is_empty() {
        return Err("producer emitted no batches for the all_scalars fixture".into());
    }
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows != 2 {
        return Err(format!("expected 2 all_scalars fixture rows, got {total_rows}").into());
    }

    let file = std::fs::File::create(out)?;
    let mut writer = StreamWriter::try_new(BufWriter::new(file), &wire_schema)?;
    for batch in &batches {
        writer.write(batch)?;
    }
    writer.finish()?;

    eprintln!(
        "wrote golden Arrow IPC stream ({total_rows} rows) to {}",
        out.display()
    );
    Ok(())
}

/// Emit the `keyvalue.flightdata` **FlightData message sequence** golden (issue
/// #2193). Drives the SAME [`FlightDataEncoderBuilder`] construction production's
/// `encode_do_get` uses (`FlightDataEncoderBuilder::new().with_schema(wire_schema)
/// .build(batch_stream)`), then length-delimits the resulting protobuf
/// `FlightData` messages so arrow-java can split and Flight-decode them.
fn emit_flightdata_golden(out: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let schema = fx::keyvalue_schema();
    let (_temp, data_dir) = build_fixture(&schema, fx::keyvalue_mutations())?;

    // Same producer + wire schema (carrying the `cqlite:pushdown` field metadata)
    // the server's `do_get` uses. `KEYVALUE_BATCH_SIZE` matches BOTH the field
    // flight image AND `do_get_transport_test.rs`'s `CqliteFlightService::new`
    // batch size, so a future larger fixture's batch boundaries stay aligned
    // with what the golden and the real-transport pin actually exercise.
    let producer = MergeProducer::new(schema, fx::KEYVALUE_BATCH_SIZE)?;
    let wire_schema = Arc::new(producer.arrow_schema()?);
    // Byte-pin safety gate (issue #2285): this golden is byte-compared on the wire
    // (unlike the semantically-decoded `all_scalars.arrows`), so its schema must
    // NOT carry a field whose metadata order is process-random. Fail regeneration
    // loudly if a future fixture introduces a >= 2-metadata-key field.
    fx::assert_wire_deterministic_metadata(&wire_schema)?;
    let table_dir = data_dir.join(fx::KEYVALUE_KS).join(fx::KEYVALUE_TBL);
    let batches = producer.produce(&DirSource::new(table_dir))?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows != fx::KEYVALUE_ROWS.len() {
        return Err(format!(
            "expected {} field fixture rows, got {total_rows}",
            fx::KEYVALUE_ROWS.len()
        )
        .into());
    }

    // Encode via the SAME builder path as `streaming.rs::encode_do_get`. Collect
    // the full FlightData sequence (schema message + record-batch message(s)).
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let input = futures::stream::iter(batches.into_iter().map(Ok::<RecordBatch, FlightError>));
    let encoder = FlightDataEncoderBuilder::new()
        .with_schema(wire_schema)
        .build(input);
    let messages: Vec<FlightData> = rt.block_on(encoder.try_collect())?;

    // The field shape (3 rows, one batch, no dictionaries) yields exactly the
    // schema message + one record-batch message. Pin it so a drift fails regen.
    if messages.len() != 2 {
        return Err(format!(
            "expected 2 FlightData messages (schema + 1 record batch), got {}",
            messages.len()
        )
        .into());
    }

    // Length-delimited protobuf: each message is prefixed with a varint length so
    // the Java side reads them with `Flight.FlightData.parseDelimitedFrom`.
    let mut buf = Vec::new();
    for msg in &messages {
        msg.encode_length_delimited(&mut buf)?;
    }
    std::fs::write(out, &buf)?;

    eprintln!(
        "wrote golden FlightData sequence ({} messages, {total_rows} rows) to {}",
        messages.len(),
        out.display()
    );
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let arrows_out = args
        .next()
        .map(PathBuf::from)
        .ok_or("usage: emit_arrow_golden <arrows-out> [<flightdata-out>]")?;
    let flightdata_out = args.next().map(PathBuf::from);

    emit_arrows_golden(&arrows_out)?;
    if let Some(out) = flightdata_out {
        emit_flightdata_golden(&out)?;
    }
    Ok(())
}
