//! Read-PHASE timing + resource-gauge honesty (issue #1707, AI7 of epic #1686).
//!
//! # What this pins
//!
//! `cqlite.read.duration` says a read was slow; before this issue NOTHING said
//! WHERE the time went, so "why was this query slow?" was unanswerable from metrics
//! and needed a profiler on the box. The four `cqlite.read.phase.*` histograms split
//! ONE completed scan's wall time into io / decompress / decode / merge, and the
//! three resource gauges (`cqlite.reader.fds.open`, `cqlite.wal.size`,
//! `cqlite.wal.replay.duration`) report what the readers and the write engine
//! already know.
//!
//! So the assertions here are about EMISSION AT A REAL READ SURFACE over REAL
//! Cassandra 5.0 SSTables (and a real `WriteEngine` for the WAL half) — never about
//! a helper being callable.
//!
//! # Why the io assertion needs an injected delay, and why that is not a hack
//!
//! The property is ATTRIBUTION: read time is charged to `read.phase.io`. Over a
//! small committed fixture with a warm page cache the real io time is microseconds,
//! so any share assertion would measure the HOST, not the code — a wall-clock race
//! (#2642). Arming a known, dominant per-read delay INSIDE the io seam makes the
//! verdict structural: io must dominate unless the seam is mis-wired, and no timing
//! luck changes that. The delay compiles out of any build without
//! `observability-testing`.
//!
//! # NO wall-clock thresholds
//!
//! Every assertion is structural or RELATIVE (a share, a sample count, presence vs
//! absence). Nothing asserts "phase X took less than N ms".
//!
//! # Fixture contract (#3220)
//!
//! Both SSTable fixtures are COMMITTED to git, so absence is a resolution defect,
//! never a legitimate skip: resolution failure is a hard FAILURE, per case, and a
//! present-but-empty scan (0 rows) fails too.
//!
//! # Why every test is serial
//!
//! The metric capture provider is process-global and uses DELTA temporality, so a
//! concurrent flow would land in another test's collect window; the armed io delay
//! is process-global for the same structural reason (the seam runs on several
//! threads of one scan).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core --features observability-testing \
//!   --test issue_1707_read_phase_timings
//! ```

#![cfg(feature = "observability-testing")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use cqlite_core::observability::catalog;
use cqlite_core::observability::read_phase::io_delay;
use cqlite_core::observability::testing::{self, CapturedMetrics};
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::{Config, Platform, TableId};

#[path = "support/datasets_root.rs"]
mod datasets_root;

/// The four phase metrics, as `(name, human label)`.
const PHASES: [(&str, &str); 4] = [
    (catalog::READ_PHASE_IO, "io"),
    (catalog::READ_PHASE_DECOMPRESS, "decompress"),
    (catalog::READ_PHASE_DECODE, "decode"),
    (catalog::READ_PHASE_MERGE, "merge"),
];

/// A COMMITTED compressed BIG (`nb`) fixture: 3 partitions, 892 on-disk rows, with a
/// `CompressionInfo.db`, so a scan really decompresses chunks.
const COMPRESSED: (&str, &str) = ("test_big", "wide_partition");

/// A COMMITTED **UNCOMPRESSED** BIG (`nb`) fixture (no `CompressionInfo.db`): 1
/// partition, 600 on-disk rows. Uncompressed is a first-class read path (#1406) and
/// it is the case that proves decompress is reported ABSENT, never as a 0.0 sample.
const UNCOMPRESSED: (&str, &str) = ("test_comp", "uncompressed_table");

/// Resolve a committed fixture's `Data.db`. FAIL-CLOSED (#3220).
fn committed_data_db((keyspace, table): (&str, &str)) -> PathBuf {
    let root = datasets_root::sstables_root_for_table(keyspace, table).unwrap_or_else(|| {
        panic!(
            "{keyspace}.{table} is COMMITTED to git and must resolve in every checkout, \
             unconditionally (#3220) — {}",
            datasets_root::describe_search(keyspace, table)
        )
    });
    let ks_dir = root.join(keyspace);
    let prefix = format!("{table}-");
    let gen_dir = std::fs::read_dir(&ks_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", ks_dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with(&prefix))
        })
        .unwrap_or_else(|| panic!("no {prefix}* generation dir under {}", ks_dir.display()));
    std::fs::read_dir(&gen_dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", gen_dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with("-Data.db"))
        })
        .unwrap_or_else(|| panic!("no *-Data.db under {}", gen_dir.display()))
}

async fn open_reader(fixture: (&str, &str)) -> Arc<SSTableReader> {
    let data_db = committed_data_db(fixture);
    let config = Config::default();
    let platform = Arc::new(
        Platform::new(&config)
            .await
            .expect("platform initialisation"),
    );
    Arc::new(
        SSTableReader::open(&data_db, &config, platform)
            .await
            .expect("open the committed fixture"),
    )
}

/// Run a full batched scan of `fixture` to completion and return the rows delivered.
/// The batched surface is the one the streaming `SELECT` executor consumes, and it
/// routes through the windowed scan driver — the dominant `SELECT *` path.
async fn full_scan(reader: Arc<SSTableReader>, keyspace: &str, table: &str) -> usize {
    let tid = TableId::new(format!("{keyspace}.{table}"));
    let mut rows = 0usize;
    let mut stream = reader.scan_stream_batched(tid, None, None, None, 64);
    while let Some(item) = stream.recv().await {
        rows += item.expect("batched scan stream item").len();
    }
    rows
}

/// Number of recorded SAMPLES for a histogram metric (0 when absent). `count` is
/// `Some(n)` only for histogram points, so this can never read a counter's
/// aggregated value as a sample count.
fn samples(metrics: &CapturedMetrics, name: &str) -> u64 {
    metrics
        .find(name)
        .map(|m| m.points.iter().filter_map(|p| p.count).sum())
        .unwrap_or(0)
}

/// Sum of recorded seconds for a phase (0.0 when absent).
fn seconds(metrics: &CapturedMetrics, name: &str) -> f64 {
    metrics.counter_sum(name)
}

fn metric_names(metrics: &CapturedMetrics) -> Vec<String> {
    metrics.entries().iter().map(|m| m.name.clone()).collect()
}

/// Assert the emission GRAIN and the cardinality contract for every phase present:
/// exactly ONE sample per phase per completed scan (never one per chunk or per row),
/// the catalogued unit, and no attribute key beyond the one a single-SSTable read
/// could honestly know.
fn assert_phase_grain_and_attributes(metrics: &CapturedMetrics, phase: &str) {
    for (name, label) in PHASES {
        let Some(entry) = metrics.find(name) else {
            continue;
        };
        assert_eq!(
            samples(metrics, name),
            1,
            "[{phase}] {label} must record EXACTLY ONE sample for ONE completed scan \
             (the whole point of accumulating at the seams and emitting once); \
             points: {:?}",
            entry.points
        );
        assert_eq!(
            metrics.unit(name),
            Some(catalog::unit::SECONDS),
            "[{phase}] {label} must carry the catalogued base-unit seconds"
        );
        for point in &entry.points {
            for (key, _) in &point.attributes {
                assert_eq!(
                    key.as_str(),
                    catalog::attr::SSTABLE_FORMAT,
                    "[{phase}] {label} may carry no attribute other than the bounded \
                     {} — every dimension multiplies cardinality; points: {:?}",
                    catalog::attr::SSTABLE_FORMAT,
                    entry.points
                );
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(read_metrics)]
async fn a_slowed_data_db_read_is_attributed_to_the_io_phase() {
    let mc = testing::metrics_capture();
    let (keyspace, table) = COMPRESSED;
    let reader = open_reader(COMPRESSED).await;

    // 8ms per instrumented read, so the io phase is dominated by a KNOWN quantity
    // and the verdict cannot turn on host timing.
    let _armed = io_delay::arm(Duration::from_millis(8));
    mc.reset();
    let rows = full_scan(Arc::clone(&reader), keyspace, table).await;
    let metrics = mc.flush_and_collect();

    assert!(
        rows > 0,
        "the committed fixture is present but the scan delivered 0 rows — a \
         dataset-dependent assertion must never pass on an empty read"
    );

    let io = seconds(&metrics, catalog::READ_PHASE_IO);
    let total: f64 = PHASES.iter().map(|(n, _)| seconds(&metrics, n)).sum();
    assert!(
        samples(&metrics, catalog::READ_PHASE_IO) > 0,
        "cqlite.read.phase.io must be recorded for a completed scan; collected \
         metrics: {:?}",
        metric_names(&metrics)
    );
    assert!(
        total > 0.0,
        "no read phase was recorded at all for a completed scan; collected metrics: \
         {:?}",
        metric_names(&metrics)
    );
    assert!(
        io / total > 0.5,
        "with 8ms of delay deliberately injected at every Data.db read, the io phase \
         must dominate the recorded phase time — io={io}s of total={total}s. A share \
         at or below half means the read seam is not attributing to io. Phase \
         seconds: {:?}",
        PHASES
            .iter()
            .map(|(n, l)| (*l, seconds(&metrics, n)))
            .collect::<Vec<_>>()
    );

    assert_phase_grain_and_attributes(&metrics, "slowed compressed scan");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(read_metrics)]
async fn a_compressed_scan_records_io_decompress_and_decode() {
    let mc = testing::metrics_capture();
    let (keyspace, table) = COMPRESSED;
    let reader = open_reader(COMPRESSED).await;

    mc.reset();
    let rows = full_scan(Arc::clone(&reader), keyspace, table).await;
    let metrics = mc.flush_and_collect();
    assert!(rows > 0, "the committed fixture delivered 0 rows");

    for (name, label) in [
        (catalog::READ_PHASE_IO, "io"),
        (catalog::READ_PHASE_DECOMPRESS, "decompress"),
        (catalog::READ_PHASE_DECODE, "decode"),
    ] {
        assert_eq!(
            samples(&metrics, name),
            1,
            "a compressed single-generation scan must record exactly one {label} \
             sample; collected metrics: {:?}",
            metric_names(&metrics)
        );
    }
    assert_eq!(
        samples(&metrics, catalog::READ_PHASE_MERGE),
        0,
        "a SINGLE-generation scan performs no k-way merge, so merge must be ABSENT \
         rather than a fabricated 0.0 sample; points: {:?}",
        metrics.find(catalog::READ_PHASE_MERGE).map(|m| &m.points)
    );
    assert_phase_grain_and_attributes(&metrics, "compressed scan");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(read_metrics)]
async fn an_uncompressed_scan_records_no_decompress_sample_at_all() {
    let mc = testing::metrics_capture();
    let (keyspace, table) = UNCOMPRESSED;
    let reader = open_reader(UNCOMPRESSED).await;

    mc.reset();
    let rows = full_scan(Arc::clone(&reader), keyspace, table).await;
    let metrics = mc.flush_and_collect();
    assert!(rows > 0, "the committed fixture delivered 0 rows");

    // io and decode still happen — the bytes are read and decoded.
    assert_eq!(
        samples(&metrics, catalog::READ_PHASE_IO),
        1,
        "an uncompressed scan still READS Data.db; collected metrics: {:?}",
        metric_names(&metrics)
    );
    assert_eq!(
        samples(&metrics, catalog::READ_PHASE_DECODE),
        1,
        "an uncompressed scan still DECODES rows; collected metrics: {:?}",
        metric_names(&metrics)
    );
    // The point of this case: an uncompressed SSTable decompresses NOTHING, so the
    // series carries no sample. A 0.0 sample would assert that a measurement was
    // taken and came back zero — a different, false claim.
    assert!(
        !metrics.contains(catalog::READ_PHASE_DECOMPRESS),
        "an UNCOMPRESSED read must record NO decompress sample (absence, never a \
         fabricated 0.0); points: {:?}",
        metrics
            .find(catalog::READ_PHASE_DECOMPRESS)
            .map(|m| &m.points)
    );
    assert_phase_grain_and_attributes(&metrics, "uncompressed scan");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(read_metrics)]
async fn the_reader_fd_gauge_reports_handles_a_scan_holds() {
    let mc = testing::metrics_capture();
    let (keyspace, table) = COMPRESSED;

    mc.reset();
    let reader = open_reader(COMPRESSED).await;
    let rows = full_scan(Arc::clone(&reader), keyspace, table).await;
    let metrics = mc.flush_and_collect();
    assert!(rows > 0, "the committed fixture delivered 0 rows");

    let entry = metrics.find(catalog::READER_FDS_OPEN).unwrap_or_else(|| {
        panic!(
            "cqlite.reader.fds.open must be reported by a read that opened SSTable \
             handles; collected metrics: {:?}",
            metric_names(&metrics)
        )
    });
    assert_eq!(
        metrics.unit(catalog::READER_FDS_OPEN),
        Some(catalog::unit::FDS),
        "the fd gauge must carry the catalogued {{fd}} unit"
    );
    assert!(
        entry.points.iter().all(|p| p.value >= 0.0),
        "an fd count is never negative — a negative reading means an increment and \
         its Drop are unpaired; points: {:?}",
        entry.points
    );
    assert!(
        entry.points.iter().any(|p| p.value > 0.0),
        "the gauge must have reported at least one NON-ZERO reading while the scan \
         held its handles open; points: {:?}",
        entry.points
    );
}

/// The WAL half of AC2, in its own module because it needs the write engine rather
/// than a committed read fixture.
#[cfg(feature = "write-support")]
mod wal_gauges {
    use super::*;
    use cqlite_core::schema::{Column, KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};

    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "obs1707".to_string(),
            table: "rows".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: std::collections::HashMap::new(),
            dropped_columns: std::collections::HashMap::new(),
        }
    }

    #[test]
    #[serial_test::serial(read_metrics)]
    fn a_writable_session_reports_wal_size_and_last_replay_duration() {
        let mc = testing::metrics_capture();
        let tmp = tempfile::TempDir::new().expect("tmp");
        let cfg = WriteEngineConfig::new(tmp.path().join("data"), tmp.path().join("wal"), schema());

        // Phase 1 — a fresh engine + one write. The engine knows its WAL size, so
        // the gauge must be reported without stat'ing anything.
        mc.reset();
        let mut engine = WriteEngine::new(cfg.clone()).expect("engine");
        engine
            .execute("INSERT INTO obs1707.rows (id, name) VALUES (1, 'one')")
            .expect("write");
        let metrics = mc.flush_and_collect();

        assert!(
            metrics.contains(catalog::WAL_SIZE),
            "cqlite.wal.size must be reported by a writable session; collected \
             metrics: {:?}",
            metric_names(&metrics)
        );
        assert_eq!(
            metrics.unit(catalog::WAL_SIZE),
            Some(catalog::unit::BYTES),
            "the WAL size gauge must carry the catalogued By unit"
        );
        assert!(
            metrics.counter_sum(catalog::WAL_SIZE) > 0.0,
            "a session that appended a mutation has a NON-EMPTY WAL; points: {:?}",
            metrics.find(catalog::WAL_SIZE).map(|m| &m.points)
        );
        assert!(
            metrics.contains(catalog::WAL_REPLAY_DURATION),
            "cqlite.wal.replay.duration must be reported at engine OPEN, including \
             the 0-entry case (a fresh WAL genuinely replayed nothing in ~0s, which \
             is a real measurement); collected metrics: {:?}",
            metric_names(&metrics)
        );
        assert_eq!(
            metrics.unit(catalog::WAL_REPLAY_DURATION),
            Some(catalog::unit::SECONDS),
            "the replay-duration gauge must carry the catalogued base-unit seconds"
        );

        // Phase 2 — REOPEN over the same WAL, which really replays the mutation
        // written above. The gauge must be reported again for the new process-open.
        drop(engine);
        mc.reset();
        let reopened = WriteEngine::new(cfg).expect("reopen engine");
        let metrics = mc.flush_and_collect();
        assert!(
            metrics.contains(catalog::WAL_REPLAY_DURATION),
            "a reopen that replays a non-empty WAL must report its replay duration; \
             collected metrics: {:?}",
            metric_names(&metrics)
        );
        assert!(
            metrics
                .find(catalog::WAL_REPLAY_DURATION)
                .is_some_and(|m| m.points.iter().all(|p| p.value >= 0.0)),
            "a replay duration is never negative; points: {:?}",
            metrics
                .find(catalog::WAL_REPLAY_DURATION)
                .map(|m| &m.points)
        );
        drop(reopened);
    }
}
