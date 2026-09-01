//! MEASUREMENT ONLY (issue #3742): does a zero-column `RecordBatch`'s explicit
//! row count survive the REAL `do_get` encode path plus an arrow-flight client
//! decode?
//!
//! Context: a zero-column projection is publicly reachable on four Flight
//! `do_get` routes and today fails, because `RecordBatch::try_new(<zero-field
//! schema>, vec![])` returns `Err("must either specify a row count or at least
//! one column")` at arrow-array 53.4.1. Posture "B" would build the batch with
//! `RecordBatchOptions::new().with_row_count(Some(n))` +
//! `RecordBatch::try_new_with_options` — arrow's sanctioned mechanism for a
//! batch whose row count is not derivable from its columns.
//!
//! B is only viable if the row count SURVIVES THE WIRE. A zero-column batch has
//! NO data buffers, so the count can only travel in the IPC `RecordBatch`
//! flatbuffer's `length` field — or nowhere. These tests measure it end to end
//! through [`crate::streaming::encode_do_get`] (the encoder the real `do_get`
//! response stream uses, `streaming.rs`) and
//! `arrow_flight::decode::FlightRecordBatchStream` (what a Trino/arrow-flight
//! client uses), plus a direct arrow-ipc `StreamWriter`/`StreamReader`
//! round-trip to locate WHERE the count travels.
//!
//! These tests change no production behaviour: they neither construct nor
//! require a zero-column batch anywhere in the shipped read path. They record a
//! transport-layer FACT the posture decision depends on.

use super::{encode_do_get, DoGetStream, StreamProbe};
use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::ipc::{root_as_message, MessageHeader};
use arrow_flight::error::FlightError;
use futures::StreamExt;
use std::sync::Arc;

const ROWS: usize = 3;

/// A zero-field schema and a zero-column batch carrying an explicit row count.
fn zero_column_batch(rows: usize) -> (Arc<ArrowSchema>, RecordBatch) {
    let schema = Arc::new(ArrowSchema::empty());
    let batch = RecordBatch::try_new_with_options(
        Arc::clone(&schema),
        vec![],
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )
    .expect("try_new_with_options must accept a zero-column batch with an explicit row count");
    (schema, batch)
}

// The stream item's `Err` is `tonic::Status`, whose size is fixed by the
// arrow-flight `FlightService` contract (#2856).
#[allow(clippy::result_large_err)]
async fn decode_all(stream: DoGetStream) -> Vec<RecordBatch> {
    let mapped = stream.map(|r| r.map_err(|s| FlightError::ExternalError(Box::new(s))));
    let mut decoded = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(mapped);
    let mut out = Vec::new();
    while let Some(b) = decoded.next().await {
        out.push(b.expect("decode a zero-column batch off the wire"));
    }
    out
}

/// Baseline, pre-transport: the arrow-side construction is legal and reports the
/// explicit row count.
#[test]
fn zero_column_batch_carries_its_row_count_before_any_transport() {
    let (schema, batch) = zero_column_batch(ROWS);
    assert_eq!(schema.fields().len(), 0, "the schema must have zero fields");
    assert_eq!(batch.num_columns(), 0, "the batch must have zero columns");
    assert_eq!(
        batch.num_rows(),
        ROWS,
        "with_row_count must be observable locally"
    );
}

/// THE MEASUREMENT: the real `do_get` encoder + a real arrow-flight client
/// decode. Whatever this asserts is the observed transport behaviour of
/// arrow/arrow-flight 53 — not a CQLite design choice.
#[test]
fn zero_column_row_count_through_the_real_do_get_encoder_and_flight_decode() {
    let (schema, batch) = zero_column_batch(ROWS);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let batches = futures::stream::iter(vec![Ok::<_, FlightError>(batch)]);
        let stream = encode_do_get(batches, schema, StreamProbe::default());
        let decoded = decode_all(stream).await;

        assert_eq!(
            decoded.len(),
            1,
            "exactly one batch must arrive off the wire, got {}",
            decoded.len()
        );
        let got = &decoded[0];
        assert_eq!(
            got.num_columns(),
            0,
            "the decoded batch must have 0 columns"
        );
        assert_eq!(
            got.num_rows(),
            ROWS,
            "MEASURED: the zero-column row count arriving off the real do_get \
             wire path (encode_do_get -> FlightRecordBatchStream)"
        );
    });
}

/// Where does the count travel? A zero-column batch has no buffers, so if the
/// arrow-ipc `StreamWriter`/`StreamReader` pair preserves it, it can only be the
/// IPC `RecordBatch` flatbuffer's `length` field.
#[test]
fn zero_column_row_count_through_arrow_ipc_stream_writer_reader() {
    let (schema, batch) = zero_column_batch(ROWS);
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, schema.as_ref())
            .expect("StreamWriter::try_new on a zero-field schema");
        w.write(&batch)
            .expect("StreamWriter::write on a zero-column batch");
        w.finish().expect("StreamWriter::finish");
    }
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(buf), None)
        .expect("StreamReader::try_new");
    let decoded: Vec<RecordBatch> = reader.map(|b| b.expect("ipc decode")).collect();
    assert_eq!(decoded.len(), 1, "one IPC batch must round-trip");
    assert_eq!(decoded[0].num_columns(), 0);
    assert_eq!(
        decoded[0].num_rows(),
        ROWS,
        "MEASURED: the row count surviving a direct arrow-ipc round-trip"
    );
}

/// GROUNDS THE PREMISE: the plain constructor really does refuse, with the exact
/// arrow-array 53.4.1 message issue #3742 names. Pinned here so a future arrow
/// bump that changes this behaviour surfaces as a failing measurement rather
/// than as a stale sentence in an issue.
#[test]
fn plain_try_new_refuses_a_zero_column_batch() {
    let err = RecordBatch::try_new(Arc::new(ArrowSchema::empty()), vec![])
        .expect_err("plain try_new must refuse a zero-column batch");
    assert!(
        err.to_string()
            .contains("must either specify a row count or at least one column"),
        "unexpected refusal text: {err}"
    );
}

/// WHERE the count travels. A zero-column batch has no data buffers, so the only
/// place a row count can ride is the IPC `Message` flatbuffer's `RecordBatch`
/// `length` field. This reads the raw `FlightData` the real encoder emits and
/// asserts exactly that: an EMPTY `data_body`, and the count in the header.
#[test]
fn zero_column_row_count_travels_in_the_ipc_message_header_not_a_buffer() {
    let (schema, batch) = zero_column_batch(ROWS);
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let batches = futures::stream::iter(vec![Ok::<_, FlightError>(batch)]);
        let mut stream = encode_do_get(batches, schema, StreamProbe::default());

        let mut record_batch_messages = 0usize;
        while let Some(msg) = stream.next().await {
            let fd = msg.expect("the encoder must not error on a zero-column batch");
            let m = root_as_message(&fd.data_header).expect("valid IPC Message header");
            if m.header_type() != MessageHeader::RecordBatch {
                continue;
            }
            record_batch_messages += 1;
            assert!(
                fd.data_body.is_empty(),
                "a zero-column batch must carry NO body bytes, got {}",
                fd.data_body.len()
            );
            let rb = m
                .header_as_record_batch()
                .expect("RecordBatch header variant");
            assert_eq!(
                rb.nodes().map(|n| n.len()).unwrap_or(0),
                0,
                "a zero-column batch must declare no field nodes"
            );
            assert_eq!(
                rb.length(),
                ROWS as i64,
                "MEASURED: the row count in the IPC RecordBatch flatbuffer `length` field"
            );
        }
        assert_eq!(
            record_batch_messages, 1,
            "exactly one RecordBatch message must be emitted"
        );
    });
}
