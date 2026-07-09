//! Emit a tiny, deterministic Arrow IPC stream **exactly as the Flight server
//! would** for a fixture covering every scalar CQL type the Trino connector
//! supports, and write it to the path given on the command line.
//!
//! This is the generator behind the Trino connector's `ArrowToTrinoGoldenTest`
//! (issue #2234). The whole point is that the golden bytes are produced by the
//! REAL server emission path — [`MergeProducer`] over a real SSTable, wrapped in
//! the server's wire [`arrow_schema`](MergeProducer::arrow_schema) (which carries
//! the uuid extension metadata, `Timestamp(Millisecond, "UTC")` unit, `Date32`,
//! and the `cqlite:pushdown` field metadata). A hand-built `VectorSchemaRoot` on
//! the Java side cannot catch schema/type drift; decoding these server bytes can.
//!
//! Regenerate with `trino-connector/scripts/regen-arrow-golden.sh` — do NOT
//! hand-edit the blob. Run:
//! `cargo run -p cqlite-flight --example emit_arrow_golden -- <out>`.

use std::collections::HashMap;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;

use cqlite_flight::producer::{DirSource, MergeProducer};

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: emit_arrow_golden <output-path>")?;

    let schema = schema();

    // Build a real single-SSTable fixture via the write engine.
    let temp = tempfile::TempDir::new()?;
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema.clone());
    let mut engine = WriteEngine::new(config)?;
    for m in mutations() {
        engine.write(m)?;
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(engine.flush())?
        .ok_or("flush produced no SSTable")?;

    // Drive the SAME producer the server's `do_get` uses, and take the SAME wire
    // schema `do_get` sends (uuid extension + Timestamp unit + pushdown metadata).
    let producer = MergeProducer::new(schema, 1024)?;
    let wire_schema = Arc::new(producer.arrow_schema()?);
    let table_dir = data_dir.join(KS).join(TBL);
    let source = DirSource::new(table_dir);
    let batches = producer.produce(&source)?;

    if batches.is_empty() {
        return Err("producer emitted no batches for the fixture".into());
    }
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if total_rows != 2 {
        return Err(format!("expected 2 fixture rows, got {total_rows}").into());
    }

    // Serialize with the server's WIRE schema (not each batch's own schema) so
    // the golden carries the exact field metadata the connector receives.
    let file = std::fs::File::create(&out)?;
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
