//! Raw `FlightServiceClient<Channel>` connect + one drained `do_get`
//! (design §Context/§(f); spec: raw-client + memory-bound requirements).
//!
//! No Trino, no JDBC, no `cqlite-core` query engine participates on this path — a
//! plain tonic channel streams `FlightData`, decoded to `RecordBatch`es that are
//! drained and dropped immediately (never collected).

use std::time::Duration;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::StreamExt;
use tonic::transport::Channel;

use crate::classify::{classify_flight_error, classify_status, Outcome};

/// The drained result of one `do_get`: its classified outcome plus the rows and
/// bytes seen while draining (0 for a non-`ok` outcome).
#[derive(Debug, Clone)]
pub struct DrainResult {
    pub outcome: Outcome,
    pub rows: u64,
    pub bytes: u64,
}

/// Connect a raw `FlightServiceClient` to `endpoint` (e.g. `http://127.0.0.1:8815`),
/// retrying the TCP connect for up to `connect_timeout` so a just-started server
/// is tolerated. Reuses the `benches/flight_do_get.rs` connect pattern.
pub async fn connect(
    endpoint: &str,
    connect_timeout: Duration,
) -> Result<FlightServiceClient<Channel>, String> {
    let channel = Channel::from_shared(endpoint.to_string())
        .map_err(|e| format!("invalid --endpoint {endpoint:?}: {e}"))?
        .connect_timeout(connect_timeout);
    // Retry the connect over the timeout window (a fresh server may not be
    // listening yet). Fixed 20ms backoff, bounded by connect_timeout.
    let deadline = std::time::Instant::now() + connect_timeout;
    loop {
        match channel.connect().await {
            Ok(c) => return Ok(FlightServiceClient::new(c)),
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    return Err(format!(
                        "could not connect to {endpoint} within {connect_timeout:?}: {e}"
                    ));
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Issue ONE `do_get` with `ticket_bytes` and DRAIN the response.
///
/// Every `RecordBatch` is consumed for its `num_rows()` and
/// `get_array_memory_size()` and then dropped before the next is polled — no
/// `Vec<RecordBatch>` is ever held, so peak memory is O(one in-flight batch)
/// regardless of result-set size (spec: memory-bound requirement).
pub async fn do_get_drain(
    client: &mut FlightServiceClient<Channel>,
    ticket_bytes: Vec<u8>,
) -> DrainResult {
    let resp = match client.do_get(Ticket::new(ticket_bytes)).await {
        Ok(resp) => resp,
        // Admission shed (#2420) and any pre-stream failure surface here.
        Err(status) => {
            return DrainResult {
                outcome: classify_status(&status),
                rows: 0,
                bytes: 0,
            }
        }
    };

    let stream = resp.into_inner().map(|r| r.map_err(FlightError::Tonic));
    let mut rb = FlightRecordBatchStream::new_from_flight_data(stream);
    let mut rows: u64 = 0;
    let mut bytes: u64 = 0;
    while let Some(next) = rb.next().await {
        match next {
            Ok(batch) => {
                rows = rows.saturating_add(batch.num_rows() as u64);
                bytes = bytes.saturating_add(batch.get_array_memory_size() as u64);
                // Explicit drop documents the drain-don't-accumulate contract;
                // `batch` would drop at loop end regardless — it is NEVER pushed
                // into a collection.
                drop(batch);
            }
            // A mid-stream error (including a mid-stream UNAVAILABLE) reclassifies
            // the whole request; partial rows/bytes are discarded.
            Err(err) => {
                return DrainResult {
                    outcome: classify_flight_error(&err),
                    rows: 0,
                    bytes: 0,
                }
            }
        }
    }
    DrainResult {
        outcome: Outcome::Ok,
        rows,
        bytes,
    }
}
