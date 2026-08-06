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
    // Pin the PROCESS-GLOBAL window open (B7). Its default is 60 s and
    // `close_if_triggered` consults `Instant::elapsed()` on every recorded access,
    // so on a contended gate a >60 s gap between these `do_get` calls would
    // auto-close the window mid-flow and the assertions would evaporate. Every
    // close here is explicit; nothing depends on elapsed time.
    partition_access::global().set_window_config(partition_access::WindowConfig {
        duration: std::time::Duration::from_secs(86_400),
        max_accesses: u64::MAX,
        ..partition_access::WindowConfig::default()
    });
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

/// L1: a partition present in SEVERAL generations is ONE access, and its byte weight
/// is the SUM of the per-generation measured gaps.
///
/// # Why this needs its own fixture
///
/// Every other fixture in the suite is single-generation, which makes the existing
/// `distinct_partitions() == 1` assertions **vacuous for k > 1**: they hold trivially
/// when there is only one SSTable to probe. Nothing in the suite would fail if
/// recording moved to a per-SSTable probe site — the spec's own "multiplies the repeat
/// count by the generation count, manufacturing concentration ⇒ bias toward go"
/// hazard — or if one generation's gap were silently dropped from the sum, its "SHALL
/// NOT silently under-report" hazard. Both need k > 1 to be visible at all.
///
/// So the fixture is two `WriteEngine` flushes of the SAME key into a temp dir, and
/// the test **affirmatively asserts k > 1 before asserting the accounting** — a
/// multi-generation test that silently landed on a single generation would be vacuous
/// in exactly the way it exists to close.
///
/// # Oracle note
///
/// CQLite writes these SSTables and CQLite reads them, and that is legitimate here.
/// The #3042 symmetric-oracle rule binds on-disk FRAMING properties, where a writer
/// and reader can make the same mistake and cancel. The property under test is
/// CQLite's own read-path access ACCOUNTING across generations — a counter cannot
/// cancel against an encoder. The expected byte total is derived from the SSTables'
/// own resolved extents, not from `AccessWeightBuilder`, so the sum clause is not
/// evidenced by the helper that computes it.
///
/// # Verified discriminating
///
/// Both hazards were reproduced against this test before it was accepted, on the
/// builder this service actually takes (the WARM readers-based one):
///
/// - folding only the first generation's extent ⇒ `total_bytes` 25 against an
///   expected 50 (the silent under-report);
/// - recording once per candidate instead of once per logical read ⇒ 3 accesses
///   against an expected 1 (the manufactured concentration).
///
/// The first attempt at this test passed under BOTH mutations, because the fixture's
/// `generations > 1` check proves two SSTables exist without proving both HOLD the
/// key — see the second affirmative check below, which is what makes the sum clause
/// falsifiable.
///
/// # Residual this case does NOT cover
///
/// The byte clause is SELECTED by measurement, so if the Flight warm path ever stopped
/// pricing at all, this case would take the fail-closed arm and stay green. It is not
/// the pin for "pricing works": that is
/// `cqlite-core/tests/issue_2827_partition_access_bytes.rs::a_big_access_is_counted_once_and_priced_from_its_measured_gap`,
/// a different fixture on a different path, which asserts a non-zero measured extent
/// and `unavailable == 0` unconditionally. What THIS case pins is the per-generation
/// ACCOUNTING — one access, summed extents — which nothing else can see.
#[tokio::test]
async fn a_partition_in_several_generations_is_one_access_weighing_their_summed_gaps() {
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};

    let _guard = PROBE.lock().await;

    // Two flushes of the SAME partition key → two generations that both hold it.
    let temp = tempfile::TempDir::new().expect("temp dir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");
    let config = WriteEngineConfig::new(data_dir.clone(), wal_dir, fx::keyvalue_schema());
    let mut engine = WriteEngine::new(config).expect("write engine");
    for generation in 0..2 {
        // Same key each time, different value so the flushes are not deduplicated
        // away; plus a filler key so neither generation is degenerate.
        engine
            .write(fx::keyvalue_write("k1", &format!("v{generation}")))
            .expect("write target key");
        engine
            .write(fx::keyvalue_write(&format!("filler{generation}"), "x"))
            .expect("write filler");
        engine
            .flush()
            .await
            .expect("flush")
            .expect("flush produced an SSTable");
    }

    // AFFIRMATIVE fixture check, BEFORE any accounting assertion: count the Data.db
    // files on disk. Inferring k from "we called flush twice" is exactly the
    // unmeasured assumption that would make this test vacuous.
    let generations = count_data_db(&data_dir);
    assert!(
        generations > 1,
        "this case is meaningless on a single generation — the assertions below are \
         trivially true at k == 1. Found {generations} Data.db file(s) under {}",
        data_dir.display()
    );

    let svc = CqliteFlightService::new(data_dir.clone(), fx::KEYVALUE_BATCH_SIZE);

    // The independently-derived oracle: each generation's own measured extent for
    // this key, summed. Resolved from the SSTables directly, NOT via the accumulator
    // under test.
    let per_generation = per_generation_extents_for_key(&data_dir, "k1").await;
    let holding: Vec<u64> = per_generation.iter().copied().filter(|b| *b > 0).collect();
    let extents_resolvable = holding.len() > 1;
    // SECOND affirmative check, and the one that actually protects the sum clause.
    // `generations > 1` above only proves two SSTables exist — it does NOT prove both
    // HOLD this key, and if only one does then a summed weight and a single-generation
    // weight are identical and the byte assertion is vacuous. (Measured: that is
    // exactly what happened on the first attempt at this test, which passed with a
    // generation's contribution deliberately dropped.)
    let expected_bytes: u64 = holding.iter().sum();

    let summary = window_over(|| async {
        assert!(
            point_read(&svc, "k1").await > 0,
            "the point read must return rows"
        );
    })
    .await
    .expect("the point read must have been recorded");

    assert_eq!(
        summary.total_accesses(),
        1,
        "ONE logical read of a partition held by {generations} generations is ONE \
         access — a per-SSTable recording site would report {generations}"
    );
    assert_eq!(
        summary.distinct_partitions(),
        1,
        "and one distinct partition"
    );
    assert_eq!(
        summary.bucket(RepeatBucket::One).distinct(),
        1,
        "in the singleton bucket — a per-SSTable site would land it in `2` and \
         manufacture a cacheable repeat"
    );
    // The access-count clauses above hold in EVERY build. The byte-sum clause needs
    // the seek/extent machinery, which the alternate `tombstones` build compiles out —
    // so the expectation is selected by an affirmative measurement of what the fixture
    // could actually resolve, and BOTH branches assert something falsifiable rather
    // than one of them skipping.
    if extents_resolvable {
        assert_eq!(
            summary.total_bytes(),
            expected_bytes,
            "the access must weigh the SUM of every generation's measured extent; a \
             dropped generation would under-report and flatter the cache \
             (per-generation extents: {per_generation:?})"
        );
        assert_eq!(
            summary.unavailable_partitions(),
            0,
            "a fully measured access must not be marked unavailable"
        );
    } else {
        // No extent was resolvable, so the probe must FAIL CLOSED rather than publish
        // a partial or zero weight as a measurement. That this branch is not silently
        // taken on the default build is pinned by
        // `issue_2827_partition_access_bytes.rs::a_big_access_is_counted_once_and_priced_from_its_measured_gap`,
        // which asserts `unavailable == 0` and a non-zero measured extent there.
        assert_eq!(
            summary.unavailable_partitions(),
            1,
            "an unpriceable access must be reported unavailable, not priced \
             (per-generation extents: {per_generation:?})"
        );
        assert_eq!(summary.total_bytes(), 0, "and must contribute no bytes");
    }
}

/// Data.db files under a data dir, recursively — the affirmative generation count.
fn count_data_db(data_dir: &std::path::Path) -> usize {
    fn walk(dir: &std::path::Path, found: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, found);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
            {
                *found += 1;
            }
        }
    }
    let mut found = 0;
    walk(data_dir, &mut found);
    found
}

/// Sum each generation's OWN measured extent for `key`, resolved per SSTable through
/// the reader — independent of `AccessWeightBuilder`, so it can contradict it.
async fn per_generation_extents_for_key(data_dir: &std::path::Path, key: &str) -> Vec<u64> {
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use cqlite_core::storage::write_engine::mutation::PartitionKey;
    use cqlite_core::{types::Value, Config};

    let schema = fx::keyvalue_schema();
    let key_bytes = PartitionKey::new(vec![(
        "key".to_string(),
        Value::Text(key.as_bytes().to_vec().into()),
    )])
    .to_bytes(&schema)
    .expect("encode partition key");

    let mut paths: Vec<std::path::PathBuf> = Vec::new();
    fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
            {
                out.push(path);
            }
        }
    }
    walk(data_dir, &mut paths);
    paths.sort();

    let config = Config::default();
    let platform = std::sync::Arc::new(
        cqlite_core::platform::Platform::new(&config)
            .await
            .expect("platform"),
    );
    let mut per_generation = Vec::new();
    for path in paths {
        let reader = SSTableReader::open(&path, &config, platform.clone())
            .await
            .expect("open generation");
        per_generation.push(
            reader
                .measured_partition_extent_for_test(&key_bytes)
                .await
                .unwrap_or(0),
        );
    }
    per_generation
}
