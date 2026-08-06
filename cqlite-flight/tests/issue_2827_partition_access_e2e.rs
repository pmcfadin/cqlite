//! Issue #2827 — end-to-end WIRING EVIDENCE for the bounded partition
//! access-distribution probe, through a real `CqliteFlightService` `do_get`.
//!
//! # Why this file exists
//!
//! A feature is done only when its PUBLIC surface exercises it. Green unit tests on
//! the recorder alone would prove the counting structure works and nothing about
//! whether the read path ever calls it. This drives repeated keyed point reads
//! through the actual gRPC service method, with the probe enabled, and reads the
//! recovered histogram back — the named surface
//! (`cqlite_core::observability::partition_access`), the call chain (point-read
//! boundary → `record_partition_access` → window close), and the observable result.
//!
//! # Its own test binary, deliberately
//!
//! The `observability-testing` capture harness installs a PROCESS-GLOBAL in-memory
//! meter provider on first use, and the probe's enable flag and measurement window
//! are likewise process-global. Sharing either with `cqlite-flight`'s parallel
//! `--lib` unit-test binary would risk cross-test contamination, so this is a
//! separate binary (the same rationale as `metrics_capture_test.rs`), and the two
//! cases here serialize on one lock.
//!
//! # Scope statement
//!
//! This change delivers **the instrument and the procedure, not the field number**.
//! Issue #2827's original AC2 (the 64–128 MiB decoded-partition-cache go/no-go) is
//! **NOT satisfied** by it — not waived, not deferred: satisfiable on the first real
//! keyed workload run with the probe enabled, and blocked only by the absence of
//! such a workload (`docs/research/phase2-verify-caching.md:214-216`). The fixture
//! here is one this repository generated, so nothing it reports is evidence about a
//! real workload — the decision procedure refuses a self-generated window by
//! construction.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-flight --features observability-testing,test-util \
//!   --test issue_2827_partition_access_e2e
//! ```

#![cfg(all(feature = "observability-testing", feature = "test-util"))]

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::Ticket;
use cqlite_core::observability::partition_access::{self, RepeatBucket, WindowSummary};
use cqlite_core::observability::{catalog, testing};
use cqlite_flight::service::CqliteFlightService;
use cqlite_flight::test_fixtures as fx;
use futures::StreamExt;
use tokio::sync::Mutex;
use tonic::Request;

/// The probe's enable flag and window are process-global; both cases take this.
static PROBE: Mutex<()> = Mutex::const_new(());

const DDL: &str = "CREATE TABLE cassandra_easy_stress.keyvalue (key text PRIMARY KEY, value text)";

/// Build the committed 1-SSTable `cassandra_easy_stress.keyvalue` fixture.
async fn build_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};

    let temp = tempfile::TempDir::new().expect("temp dir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, fx::keyvalue_schema());
    let mut engine = WriteEngine::new(config).expect("write engine");
    for mutation in fx::keyvalue_mutations() {
        engine.write(mutation).expect("write mutation");
    }
    engine
        .flush()
        .await
        .expect("flush")
        .expect("flush produced an SSTable");
    (temp, data_dir)
}

/// A ticket carrying a FULL-PK equality `filter` — the shape that actually routes
/// to the point path.
///
/// The mechanism is easy to get wrong and worth stating: **tokens do not route, the
/// filter does.** A ticket with correct key-derived token bounds and no `filter`
/// resolves to `full_scan` — correct rows at scan latency — so this asserts the
/// reported access path rather than trusting the ticket shape.
fn point_ticket_bytes(key: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "keyspace": fx::KEYVALUE_KS,
        "table": fx::KEYVALUE_TBL,
        "ddl": DDL,
        "filter": { "type": "Compare", "column": "key", "op": "Equal", "value": key },
    }))
    .expect("ticket json")
}

/// Drive one point-read `do_get` to completion, returning the rows observed.
async fn point_read(svc: &CqliteFlightService, key: &str) -> usize {
    let mut stream = svc
        .do_get(Request::new(Ticket::new(point_ticket_bytes(key))))
        .await
        .expect("do_get")
        .into_inner();
    let mut messages = 0usize;
    while let Some(msg) = stream.next().await {
        msg.expect("flight data message");
        messages += 1;
    }
    messages
}

/// Enable the probe, run `flow`, close the window deterministically, disable again.
async fn window_over<F, Fut>(flow: F) -> Option<WindowSummary>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    // Discard anything a sibling left open, then start clean.
    let _ = partition_access::close_window();
    partition_access::set_probe_enabled(Some(true));
    flow().await;
    let summary = partition_access::close_window();
    partition_access::set_probe_enabled(Some(false));
    summary
}

/// Repeated keyed `do_get` point reads produce the histogram, on the keyed route.
#[tokio::test]
async fn repeated_keyed_point_reads_through_do_get_produce_the_histogram() {
    let _guard = PROBE.lock().await;
    let capture = testing::metrics_capture();
    capture.reset();
    let (_temp, data_dir) = build_fixture().await;
    let svc = CqliteFlightService::new(data_dir, fx::KEYVALUE_BATCH_SIZE);

    // One partition hit five times, two others once each. Hand-computed
    // expectation: `5-8` holds 1 distinct partition with 5 accesses; `1` holds 2
    // distinct partitions with 1 access each.
    let summary_opt = window_over(|| async {
        for _ in 0..5 {
            assert!(point_read(&svc, "k1").await > 0);
        }
        assert!(point_read(&svc, "k2").await > 0);
        assert!(point_read(&svc, "k3").await > 0);
    })
    .await;

    // The instrument must have sat on the KEYED route, not a degraded scan. A plain
    // full-PK equality reports `streaming_partition_lookup` on
    // `cqlite.query.rows_scanned`; bare `partition_lookup` is a label this route
    // never emits, and `full_scan` is what a ticket whose filter failed to route
    // would report — in which case the histogram would be measuring the wrong thing.
    let summary = summary_opt.expect("the probe must have recorded the keyed point reads");
    let metrics = capture.flush_and_collect();
    assert!(
        metrics.sum_where(
            catalog::QUERY_ROWS_SCANNED,
            &[(catalog::attr::ACCESS_PATH, "streaming_partition_lookup")]
        ) > 0.0,
        "the reads must report the keyed point route"
    );
    assert_eq!(
        metrics.sum_where(
            catalog::QUERY_ROWS_SCANNED,
            &[(catalog::attr::ACCESS_PATH, "full_scan")]
        ),
        0.0,
        "no read may have degraded to a full scan"
    );

    assert_eq!(
        summary.distinct_partitions(),
        3,
        "three distinct partitions were touched, whatever the read count"
    );
    assert_eq!(
        summary.bucket(RepeatBucket::FiveToEight).distinct(),
        1,
        "the five-times partition lands in 5-8"
    );
    assert_eq!(summary.bucket(RepeatBucket::FiveToEight).accesses, 5);
    assert_eq!(
        summary.bucket(RepeatBucket::One).distinct(),
        2,
        "the two single-read partitions land in 1"
    );
    assert_eq!(summary.bucket(RepeatBucket::One).accesses, 2);
    assert_eq!(summary.total_accesses(), 7);
    for empty in [
        RepeatBucket::Two,
        RepeatBucket::ThreeToFour,
        RepeatBucket::NineToSixteen,
        RepeatBucket::SeventeenPlus,
    ] {
        assert_eq!(
            summary.bucket(empty).distinct(),
            0,
            "bucket {} must be empty for this access pattern",
            empty.label()
        );
    }
}

/// With the probe unset, a keyed workload emits nothing and allocates nothing.
#[tokio::test]
async fn the_probe_is_off_by_default_and_costless_when_off() {
    let _guard = PROBE.lock().await;
    let (_temp, data_dir) = build_fixture().await;
    let svc = CqliteFlightService::new(data_dir, fx::KEYVALUE_BATCH_SIZE);

    // `None` returns the process to resolving from the environment, which is unset
    // in the test environment — i.e. the production default.
    let _ = partition_access::close_window();
    partition_access::set_probe_enabled(None);
    assert!(
        !partition_access::enabled(),
        "CQLITE_PARTITION_ACCESS_PROBE is unset, so the probe must be OFF"
    );

    // Footprint BEFORE the disabled workload. This is deliberately a delta, not an
    // absolute: the recorder is process-global, so a sibling case in this binary may
    // already have allocated the (fixed) table. The zero-footprint-from-cold
    // property is asserted on a FRESH recorder in
    // `cqlite-core/src/observability/partition_access` (`a_disabled_recorder_allocates_nothing`),
    // where it is meaningful; what is order-independent HERE is that a disabled
    // probe adds nothing.
    let footprint_before = partition_access::global().footprint_bytes();

    let rows_before = point_read(&svc, "k1").await;
    for _ in 0..4 {
        point_read(&svc, "k1").await;
    }

    assert!(rows_before > 0, "query results are unchanged by the probe");
    assert_eq!(
        partition_access::close_window(),
        None,
        "a disabled probe records nothing, so there is no window to emit"
    );
    assert_eq!(
        partition_access::global().footprint_bytes(),
        footprint_before,
        "a disabled keyed workload must not allocate or grow the counting table"
    );
}
