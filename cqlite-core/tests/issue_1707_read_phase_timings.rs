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

use cqlite_core::config::DiskAccessMode;
use cqlite_core::observability::catalog;
use cqlite_core::observability::io_delay;
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
    open_reader_with(fixture, Config::default()).await
}

async fn open_reader_with(fixture: (&str, &str), config: Config) -> Arc<SSTableReader> {
    let data_db = committed_data_db(fixture);
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

/// The LAST reported value of a gauge in this collect window, or `None` when the
/// series is absent or carries no point.
///
/// `Option`, deliberately: a "the level came back down" assertion needs the final
/// reading, and an earlier form folded the points with `fold(f64::NAN, |_, v| v)`,
/// which yields `NaN` for an empty point list and then fails `assert_eq!(NaN, 0.0)`
/// opaquely — a real absence reported as an arithmetic mismatch.
fn last_gauge_value(metrics: &CapturedMetrics, name: &str) -> Option<f64> {
    metrics.find(name)?.points.last().map(|p| p.value)
}

/// Assert the emission GRAIN and the cardinality contract for every phase present:
/// exactly ONE sample per phase per completed scan (never one per chunk or per row),
/// the catalogued unit, and NO attribute at all.
///
/// The attribute assertion is written against the DECLARATION, not the
/// implementation: `catalog_read_phase` says "**Attributes**: none" for all four
/// series and every `operator_docs_annotations_read_phase` row carries
/// `attributes: &[]`. An earlier form of this helper asserted each emitted key
/// EQUALS `sstable.format`, which permitted exactly the drift it was there to
/// catch — a test written from the code cannot falsify the code.
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
            let keys: Vec<&str> = point.attributes.iter().map(|(k, _)| k.as_str()).collect();
            assert!(
                keys.is_empty(),
                "[{phase}] {label} is DECLARED attribute-free (catalog_read_phase says \
                 \"Attributes: none\"; every operator_docs_annotations_read_phase row \
                 sets attributes: &[]), so it must emit NO attribute key — every \
                 dimension multiplies cardinality, and an emitted key the docs do not \
                 declare is a disagreement between the metric and its own contract; \
                 got: {keys:?}"
            );
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

/// A `Config` pinned to one disk-access backend, so the fd assertions do not depend
/// on the `Auto` heuristic (which picks mmap for any file over 4 KiB, i.e. for every
/// committed fixture — the reason this test cannot just use `Config::default()`).
fn config_with_backend(mode: DiskAccessMode) -> Config {
    let mut config = Config::default();
    config.storage.disk_access_mode = mode;
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(read_metrics)]
async fn the_reader_fd_gauge_reports_the_descriptors_a_buffered_scan_holds() {
    let mc = testing::metrics_capture();
    let (keyspace, table) = COMPRESSED;

    mc.reset();
    let reader = open_reader_with(COMPRESSED, config_with_backend(DiskAccessMode::Buffered)).await;
    let rows = full_scan(Arc::clone(&reader), keyspace, table).await;
    let metrics = mc.flush_and_collect();
    assert!(rows > 0, "the committed fixture delivered 0 rows");

    let entry = metrics.find(catalog::READER_FDS_OPEN).unwrap_or_else(|| {
        panic!(
            "cqlite.reader.fds.open must be reported by a BUFFERED read, which really \
             opens descriptors; collected metrics: {:?}",
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
        "the gauge must have reported at least one NON-ZERO level while the reader \
         and its scan held descriptors open; points: {:?}",
        entry.points
    );
    assert!(
        entry.points.iter().all(|p| p.attributes.is_empty()),
        "the fd gauge is total-only — no attributes; points: {:?}",
        entry.points
    );

    // And the level comes BACK DOWN: dropping the reader releases its handles, so
    // the last reading of a window that ends after the drop must be 0. A gauge that
    // only ever climbs is the unpaired-decrement bug this catches.
    mc.reset();
    drop(reader);
    let after = mc.flush_and_collect();
    if let Some(entry) = after.find(catalog::READER_FDS_OPEN) {
        assert_eq!(
            last_gauge_value(&after, catalog::READER_FDS_OPEN),
            Some(0.0),
            "after the last reader is dropped the gauge must read 0, not a level \
             that never falls; points: {:?}",
            entry.points
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial(read_metrics)]
async fn an_mmap_backed_read_reports_no_descriptors_rather_than_a_plausible_number() {
    // The catalog claims mmap contributes 0 because it holds a MAPPING, not a
    // descriptor. That claim is only worth making if it is pinned: an implementation
    // that counted "one fd per source" would look right on the buffered case above
    // and silently overstate fd pressure for every mmap reader.
    let mc = testing::metrics_capture();
    let (keyspace, table) = COMPRESSED;

    mc.reset();
    let reader = open_reader_with(COMPRESSED, config_with_backend(DiskAccessMode::Mmap)).await;
    let rows = full_scan(Arc::clone(&reader), keyspace, table).await;
    let metrics = mc.flush_and_collect();
    assert!(rows > 0, "the committed fixture delivered 0 rows");

    // The phases still record — this really is a full read, just without descriptors.
    assert_eq!(
        samples(&metrics, catalog::READ_PHASE_IO),
        1,
        "an mmap-backed scan still performs (and times) its Data.db reads; collected \
         metrics: {:?}",
        metric_names(&metrics)
    );
    if let Some(entry) = metrics.find(catalog::READER_FDS_OPEN) {
        assert!(
            entry.points.iter().all(|p| p.value <= 0.0),
            "an mmap-backed reader holds NO file descriptors, so this gauge must not \
             report a positive level for it (a mapping is not an fd, and an Arc clone \
             of a mapping is not one either); points: {:?}",
            entry.points
        );
    }
}

/// The WAL half of AC2, in its own module because it needs the write engine rather
/// than a committed read fixture.
#[cfg(feature = "write-support")]
mod wal_gauges {
    use super::*;
    use cqlite_core::schema::{Column, KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::{
        CellOperation, Mutation, PartitionKey, TableId as WriteTableId, WriteEngine,
        WriteEngineConfig,
    };
    use cqlite_core::types::Value;

    /// A fixed write timestamp (µs) — nothing here depends on wall clock (#2642).
    const T0: i64 = 1_704_067_200_000_000;

    fn mutation(id: i32, name: &str) -> Mutation {
        Mutation::new(
            WriteTableId::new("obs1707", "rows"),
            PartitionKey::single("id", Value::Integer(id)),
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::text(name.to_string()),
            }],
            T0,
            None,
        )
    }

    pub(super) fn schema() -> TableSchema {
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

    /// The gauge must be emitted from the ASYNC write path too, and it must COME BACK
    /// DOWN when a flush truncates the WAL.
    ///
    /// Both halves are regressions this test exists for, and the sibling test above
    /// is blind to both: it writes via `engine.execute(...)`, the SYNC path, and never
    /// flushes.
    ///
    /// * `record_wal_gauges` had a single call site inside the sync
    ///   `write_into_memtable`, and `write_async_inner` duplicates that logic — so an
    ///   async-API caller got NO `cqlite.wal.size` series at all.
    /// * the post-flush truncate emitted nothing, so the gauge only ever CLIMBED,
    ///   while the operator doc promises a saw-tooth and reads a monotonic climb as
    ///   "flushes are not keeping up". A working flush therefore manufactured an
    ///   alarm.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(read_metrics)]
    async fn the_async_write_path_reports_wal_size_and_a_flush_brings_it_back_down() {
        let mc = testing::metrics_capture();
        let tmp = tempfile::TempDir::new().expect("tmp");
        let cfg = WriteEngineConfig::new(tmp.path().join("data"), tmp.path().join("wal"), schema());
        let mut engine = WriteEngine::new(cfg).expect("engine");

        // Half 1 — the ASYNC write path alone (no `execute`, no flush).
        mc.reset();
        for (id, name) in [(1, "one"), (2, "two")] {
            engine.write_async(mutation(id, name)).await.expect("write");
        }
        let after_writes = mc.flush_and_collect();

        assert!(
            after_writes.contains(catalog::WAL_SIZE),
            "cqlite.wal.size must be reported by the ASYNC write path, not only by the \
             sync one — the two paths duplicate the same logic and a caller using \
             write_async got no series at all; collected metrics: {:?}",
            metric_names(&after_writes)
        );
        let grown = last_gauge_value(&after_writes, catalog::WAL_SIZE)
            .expect("the async write path reported a WAL size point");
        assert!(
            grown > 0.0,
            "two appended mutations leave a NON-EMPTY WAL; points: {:?}",
            after_writes.find(catalog::WAL_SIZE).map(|m| &m.points)
        );

        // Half 2 — a flush truncates the WAL, so the NEXT reported level must be
        // strictly lower than the level the writes reached. Asserting "lower than the
        // pre-flush level" rather than "== 0" keeps the assertion about the SAW-TOOTH
        // the docs promise without pinning an implementation detail of what an empty
        // WAL file weighs.
        mc.reset();
        engine
            .flush()
            .await
            .expect("flush")
            .expect("the flush produced an sstable");
        let after_flush = mc.flush_and_collect();

        let truncated = last_gauge_value(&after_flush, catalog::WAL_SIZE).unwrap_or_else(|| {
            panic!(
                "a flush TRUNCATES the WAL, so it must report the new (lower) level — \
                 without that emission the gauge only ever climbs and the operator doc's \
                 \"a level that only climbs means flushes are not keeping up\" turns a \
                 healthy flush into a false alarm; collected metrics: {:?}",
                metric_names(&after_flush)
            )
        });
        assert!(
            truncated < grown,
            "the post-flush WAL level ({truncated}) must be BELOW the pre-flush level \
             ({grown}) — that fall is the saw-tooth the operator doc promises; points: \
             {:?}",
            after_flush.find(catalog::WAL_SIZE).map(|m| &m.points)
        );

        drop(engine);
    }
}

/// The MERGE phase, which is recorded only on the CROSS-GENERATION read route — so
/// it needs two generations of one table, which the write engine can produce.
#[cfg(feature = "write-support")]
mod merge_phase {
    use super::wal_gauges::schema;
    use super::*;
    use cqlite_core::schema::registry::{SchemaRegistry, SchemaRegistryConfig, SchemaSource};
    use cqlite_core::storage::sstable::SSTableManager;
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};

    /// Build TWO generations of one table, scan them through the CROSS-GENERATION
    /// reconciling merge, and return `(rows delivered, metrics for that scan)`.
    ///
    /// Shared by the two tests below so the io case and the merge case exercise
    /// literally the same route; `arm_io_delay` makes the physical `Data.db` reads
    /// unmissably slow, so an io assertion is about WIRING and never about host
    /// timing (#2642).
    async fn scan_two_generations(arm_io_delay: bool) -> (usize, CapturedMetrics) {
        let mc = testing::metrics_capture();
        let tmp = tempfile::TempDir::new().expect("tmp");
        let data_dir = tmp.path().join("data");
        let cfg = WriteEngineConfig::new(data_dir.clone(), tmp.path().join("wal"), schema());

        // TWO generations of one table: write + flush, twice. The k-way reconciling
        // merge is what the read path takes when a table has more than one
        // generation and a schema is available.
        let mut engine = WriteEngine::new(cfg).expect("engine");
        for (id, name) in [(1, "one"), (2, "two")] {
            engine
                .execute(&format!(
                    "INSERT INTO obs1707.rows (id, name) VALUES ({id}, '{name}')"
                ))
                .expect("write");
            engine.flush().await.expect("flush").expect("sstable");
        }
        drop(engine);

        let config = Config::default();
        let platform = Arc::new(Platform::new(&config).await.expect("platform"));
        let registry = SchemaRegistry::new(
            SchemaRegistryConfig::default(),
            platform.clone(),
            config.clone(),
        )
        .await
        .expect("build registry");
        registry
            .register_schema(schema(), SchemaSource::Manual)
            .await
            .expect("register schema");
        let registry = Arc::new(tokio::sync::RwLock::new(registry));
        let manager = SSTableManager::new(&data_dir, &config, platform, Some(registry))
            .await
            .expect("open manager over both generations");

        let table_id = TableId::new("obs1707.rows".to_string());
        let schema = schema();
        let _armed = arm_io_delay.then(|| io_delay::arm(Duration::from_millis(4)));
        mc.reset();
        let mut rows = 0usize;
        {
            let mut stream = manager
                .scan_stream(&table_id, None, None, Some(&schema), 16)
                .await
                .expect("cross-generation streaming scan");
            while let Some(item) = stream.recv().await {
                item.expect("stream item");
                rows += 1;
            }
        }
        (rows, mc.flush_and_collect())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(read_metrics)]
    async fn a_cross_generation_read_records_the_merge_phase() {
        let (rows, metrics) = scan_two_generations(false).await;

        assert!(
            rows > 0,
            "the two generations hold rows, so a read that delivered none would make \
             every assertion below vacuous"
        );
        assert_eq!(
            samples(&metrics, catalog::READ_PHASE_MERGE),
            1,
            "a CROSS-GENERATION read must record exactly ONE merge sample for the \
             whole operation (the per-generation sub-scans are metered inert, so they \
             cannot add a second); collected metrics: {:?}",
            metric_names(&metrics)
        );
        assert_eq!(
            metrics.unit(catalog::READ_PHASE_MERGE),
            Some(catalog::unit::SECONDS),
            "the merge phase must carry the catalogued base-unit seconds"
        );
        assert!(
            metrics
                .find(catalog::READ_PHASE_MERGE)
                .is_some_and(|m| m.points.iter().all(|p| p.value >= 0.0)),
            "recv-wait subtraction must never produce a negative (or wrapped) merge \
             time; points: {:?}",
            metrics.find(catalog::READ_PHASE_MERGE).map(|m| &m.points)
        );
        // CQLite's own write surface emits UNCOMPRESSED SSTables (#1406), so this
        // read decompresses nothing and the series must be absent, not 0.0.
        //
        // NOTE this assertion passes for a REASON THAT IS NOT THE PROPERTY: these
        // generations are CQLite-written and therefore uncompressed, so no
        // decompression happens on any thread. It cannot distinguish "nothing to
        // decompress" from "the decompress seam is unreachable from this route", and
        // the io assertion above is what actually exercises producer-thread
        // propagation.
        assert!(
            !metrics.contains(catalog::READ_PHASE_DECOMPRESS),
            "an uncompressed cross-generation read must record NO decompress sample; \
             points: {:?}",
            metrics
                .find(catalog::READ_PHASE_DECOMPRESS)
                .map(|m| &m.points)
        );
    }
    /// KNOWN GAP (issue #1707): the cross-generation merge route records NO
    /// `read.phase.io` sample, so this test is RED and `#[ignore]`d rather than
    /// deleted.
    ///
    /// It is NOT a propagation gap — that half is fixed and shipped in this change:
    /// the sink IS captured on the merge/consumer thread and installed on both
    /// per-input producer threads (`merge::from_readers`, `merge::producer_iter`),
    /// which is what makes `decompress` reachable on this route through the shared
    /// chunk-decode plane. The gap is that THE IO SEAM DOES NOT EXIST on this read
    /// route at all: `read_phase::scoped(ReadPhase::Io)` appears only in
    /// `scan_stream_windowed_read`, and a merge producer reads through
    /// `stream_all_partitions_for_compaction` / `_for_query`, which has no io seam at
    /// any depth. Closing it means instrumenting a SECOND read route, a change to the
    /// read path rather than to metric wiring.
    ///
    /// Kept as an executable RED test because the alternative — deleting it — would
    /// leave nothing that fails the day someone believes the route is io-measured.
    /// `observability::read_phase`'s coverage boundary states the same gap in prose,
    /// so an operator is not misled in the meantime. Un-`#[ignore]` it when the seam
    /// lands; nothing else about it should need to change.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial_test::serial(read_metrics)]
    #[ignore = "issue #1707: the merge route's read path has no io seam (propagation is done; the seam is not) — un-ignore when it is instrumented"]
    async fn a_cross_generation_read_records_the_io_phase() {
        let (rows, metrics) = scan_two_generations(true).await;
        assert!(rows > 0, "a read that delivered no rows proves nothing");
        assert!(
            samples(&metrics, catalog::READ_PHASE_IO) >= 1,
            "a cross-generation read PHYSICALLY READS Data.db (with a 4ms delay armed \
             at every instrumented read), so read.phase.io must carry at least one \
             sample — an absent series on a route an operator believes is measured \
             reads as \"io was free\" on exactly the path where io is most likely the \
             problem; collected metrics: {:?}",
            metric_names(&metrics)
        );
    }
}
