//! In-process self-test (design §(g); spec: self-test requirement).
//!
//! Serves a real `CqliteFlightService` over an ephemeral loopback port
//! (`127.0.0.1:0`) backed by a tiny 1-SSTable `keyvalue` fixture, then runs a
//! concurrency-1, fixed-request-count (NON-wall-clock) ramp against it through
//! the ordinary [`run_ramp`] engine — real client → gRPC wire → server → JSONL.
//! This is WIRING EVIDENCE: it exercises the full client→server→record pipeline,
//! catching JSONL-schema drift and ramp-loop regressions a compile-only check
//! would miss.
//!
//! This is a NORMAL workspace test surface (also reachable via the `--self-test`
//! subcommand). It is NOT an `agent-gate.sh` component and never contacts a real
//! cluster. It uses no fixed port and no wall-clock-duration step, so it is
//! deterministic and free of port/timing flake.

use std::time::Duration;

use arrow_flight::flight_service_server::FlightServiceServer;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Server;

use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;

use crate::ramp::{run_ramp, RampConfig, StepBound};
use crate::record::StepRecord;
use crate::shape::{Shape, ShapeGen};

/// A running in-process self-test server: the temp dir (kept alive) plus the
/// bound `http://127.0.0.1:<port>` endpoint and the server task handle.
pub struct SelfTestServer {
    _temp: tempfile::TempDir,
    pub endpoint: String,
    pub handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl SelfTestServer {
    /// Stop the background server task.
    pub fn shutdown(self) {
        self.handle.abort();
    }
}

/// Build the tiny 1-SSTable `keyvalue` fixture and serve it over an ephemeral
/// loopback port. Returns once the server socket is bound and accepting.
pub async fn serve_fixture() -> Result<SelfTestServer, String> {
    let schema = fx::keyvalue_schema();
    let temp = tempfile::TempDir::new().map_err(|e| format!("temp dir: {e}"))?;
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, schema);
    let mut engine = WriteEngine::new(config).map_err(|e| format!("write engine: {e}"))?;
    for (key, value) in fx::KEYVALUE_ROWS {
        engine
            .write(fx::keyvalue_write(key, value))
            .map_err(|e| format!("write mutation: {e}"))?;
    }
    engine
        .flush()
        .await
        .map_err(|e| format!("flush: {e}"))?
        .ok_or_else(|| "flush produced no SSTable".to_string())?;

    let svc = CqliteFlightService::new(data_dir, fx::KEYVALUE_BATCH_SIZE);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("bind loopback: {e}"))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("local addr: {e}"))?;
    let incoming =
        TcpIncoming::from_listener(listener, true, None).map_err(|e| format!("incoming: {e}"))?;
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(svc))
            .serve_with_incoming(incoming)
            .await
    });

    Ok(SelfTestServer {
        _temp: temp,
        endpoint: format!("http://{addr}"),
        handle,
    })
}

/// The self-test ticket template: the connector-shaped `keyvalue` ticket (full
/// ring, no limit) the fixture serves.
pub fn selftest_template() -> cqlite_flight::ticket::FlightTicket {
    // `FlightTicket` is `#[non_exhaustive]`, so an external crate cannot use a
    // struct literal — construct from `default()` and assign the fields.
    let mut t = cqlite_flight::ticket::FlightTicket::default();
    t.keyspace = fx::KEYVALUE_KS.into();
    t.table = fx::KEYVALUE_TBL.into();
    t.ddl = fx::KEYVALUE_DDL.into();
    t
}

/// Run the end-to-end self-test ramp: a 1-step, concurrency-1, `request_count`
/// bounded ramp against a freshly-served in-process fixture. Returns the emitted
/// step records (one, for the single step). `request_count` must be >= 1.
pub async fn run_self_test(request_count: u64) -> Result<Vec<StepRecord>, String> {
    let server = serve_fixture().await?;
    // Full-ring `full` shape so every request returns the fixture's rows (the
    // point shape's seeded sub-range could legitimately be empty — design §(b)).
    let gen = ShapeGen::new(
        selftest_template(),
        42,
        100,
        1 << 40,
        crate::shape::MixWeights::default(),
    );
    let config = RampConfig {
        concurrencies: vec![1],
        bound: StepBound::Requests(request_count.max(1)),
        shape: Shape::Full,
        round: "self-test".into(),
        endpoint: server.endpoint.clone(),
        connect_timeout: Duration::from_secs(5),
        seed: 42,
    };
    let result = run_ramp(&config, &gen).await;
    server.shutdown();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Wiring evidence: real client → gRPC → server → JSONL. A concurrency-1,
    /// fixed-request-count ramp against the in-process fixture emits exactly one
    /// well-formed step record with `requests_ok >= 1` and every required field
    /// present and parseable. No fixed port, no wall-clock step.
    #[tokio::test]
    async fn self_test_produces_a_wellformed_jsonl_record() {
        let records = run_self_test(3).await.expect("self-test ramp");
        assert_eq!(records.len(), 1, "one step ⇒ one record");
        let rec = &records[0];
        assert_eq!(rec.target_concurrency, 1);
        assert_eq!(rec.shape, "full");
        assert!(
            rec.requests_ok >= 1,
            "the fixture must serve at least one ok do_get, got {rec:?}"
        );
        assert_eq!(
            rec.requests_error, 0,
            "no error outcomes against the healthy fixture: {:?}",
            rec.error_codes
        );
        assert!(
            rec.rows_total >= 1,
            "a full-ring scan of the 3-row fixture must drain rows"
        );

        // The record round-trips as a single valid JSONL line with all fields.
        let line = rec.to_jsonl().expect("serialize");
        assert!(!line.contains('\n'));
        let v: serde_json::Value = serde_json::from_str(&line).expect("parse");
        for field in [
            "schema",
            "target_concurrency",
            "shape",
            "duration_s",
            "requests_ok",
            "requests_unavailable",
            "requests_error",
            "qps",
            "rows_per_s",
            "bytes_per_s",
            "rows_total",
            "bytes_total",
            "latency_ms",
        ] {
            assert!(v.get(field).is_some(), "field {field} present in JSONL");
        }
        // qps == requests_ok / duration_s (spec invariant).
        let qps = v["qps"].as_f64().unwrap();
        let ok = v["requests_ok"].as_f64().unwrap();
        let dur = v["duration_s"].as_f64().unwrap();
        if dur > 0.0 {
            assert!((qps - ok / dur).abs() < 1e-6, "qps == ok/duration");
        }
    }
}
