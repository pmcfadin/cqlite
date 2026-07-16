//! Observability correctness + sampling validation (epic #1031, issue #1043).
//!
//! These tests use the shared in-memory OTLP capture fixture
//! ([`cqlite_core::observability::testing`]) to assert that representative read,
//! query, write, and compaction flows produce the EXPECTED span trees and the
//! catalog metric names/units, and that parent-based trace-ID-ratio sampling
//! behaves as configured. No collector, network, or timing dependence: every
//! flow is force-flushed before assertions.
//!
//! Feature gating: the whole file requires `observability-testing` (which pulls
//! in the SDK in-memory exporters). The read/query flows additionally need
//! `cli-helpers`; the write/compaction flows need `write-support` (on by default).
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core \
//!   --features observability-testing,cli-helpers --test observability_correctness
//! ```
//!
//! # Why metric assertions live in one serial test
//!
//! The production metric helpers record through a single process-global `Meter`
//! that binds on first use, so the in-memory meter provider is process-wide and
//! cannot be swapped per test. All metric assertions therefore run in ONE test
//! that resets the capture, runs its flows, and reads back — avoiding cross-test
//! races on the shared provider. Span assertions, by contrast, use a per-call
//! isolated tracer and can be split across tests freely.

#![cfg(feature = "observability-testing")]

use cqlite_core::error::Error;
use cqlite_core::observability::{self as obs, catalog, testing, ErrorCategory};

/// Install (idempotently) the process-global in-memory meter provider.
///
/// The production metric helpers bind a single global `Meter` on FIRST use, so
/// whichever test records a metric first determines the provider for the whole
/// process. Every test here that runs an instrumented flow (which emits catalog
/// metrics) calls this first so the global meter is always the in-memory one,
/// regardless of test order or parallelism. The metric-assertion test additionally
/// `reset`s + `flush_and_collect`s around its own emissions.
fn ensure_meter_installed() {
    let _ = testing::metrics_capture();
}

// ---------------------------------------------------------------------------
// Span tree correctness — read + query flow (cli-helpers)
// ---------------------------------------------------------------------------

/// A read/query flow through the public `Database::execute` API produces a
/// `query.execute` span that parents the read-path spans (issue #1034/#1035).
#[cfg(feature = "cli-helpers")]
#[test]
fn query_execute_span_parents_read_path() {
    ensure_meter_installed();
    let fx = read_fixtures::SIMPLE;
    let loaded = read_fixtures::open_read_db(&fx);
    let sql = format!("SELECT * FROM {}", fx.qualified());

    // capture_spans installs an isolated tracer as the thread-local default; run
    // the async flow on a CURRENT-THREAD runtime so every instrumented future is
    // polled on this thread and seen by that subscriber.
    let spans = testing::capture_spans(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");
        let res = rt
            .block_on(loaded.db.execute(&sql))
            .expect("read flow query");
        assert!(!res.rows.is_empty(), "read flow returned zero rows");
    });

    assert!(
        spans.contains("query.execute"),
        "expected a query.execute span; saw: {:?}",
        span_names(&spans)
    );

    // At least one read-path span (the query.* sub-spans or storage/sstable
    // spans) must nest under query.execute. We assert the SELECT plan span, which
    // is created inside execute for SELECTs.
    let read_child = spans.iter().map(|s| s.name.as_str()).find(|n| {
        n.starts_with("query.") && *n != "query.execute"
            || n.starts_with("sstable.")
            || n.starts_with("storage.")
    });
    assert!(
        read_child.is_some(),
        "expected a read-path child span under query.execute; saw: {:?}",
        span_names(&spans)
    );
    let child = read_child.expect("read child span present");
    assert!(
        spans.is_parent_of("query.execute", child),
        "query.execute must parent {child}; saw spans: {:?}",
        span_tree(&spans)
    );
}

// ---------------------------------------------------------------------------
// Span tree correctness — write flow (write-support)
// ---------------------------------------------------------------------------

/// A write flow (ingest one row, flush) produces the instrumented write spans
/// (`write.mutation`, `flush.public` / `flush.memtable`).
#[cfg(feature = "write-support")]
#[test]
fn write_flow_emits_write_spans() {
    ensure_meter_installed();
    let spans = testing::capture_spans(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let mut engine = write_helpers::open_engine(tmp.path());
        engine
            .execute("INSERT INTO obs_ks.items (id, name, score) VALUES (1, 'one', 10)")
            .expect("write row");
        let info = rt
            .block_on(engine.flush())
            .expect("flush")
            .expect("sstable");
        assert!(info.data_path.exists(), "flush must produce a Data.db");
    });

    // The CQL write path is instrumented as `write.cql_execute`, which in turn
    // crosses `memtable.insert` + `wal.append`/`wal.sync`.
    assert!(
        spans.contains("write.cql_execute"),
        "expected a write.cql_execute span; saw: {:?}",
        span_names(&spans)
    );
    assert!(
        spans.contains("memtable.insert"),
        "expected a memtable.insert span; saw: {:?}",
        span_names(&spans)
    );
    assert!(
        spans.contains("flush.public") || spans.contains("flush.memtable"),
        "expected a flush span; saw: {:?}",
        span_names(&spans)
    );
    // The flush writer path is instrumented too.
    assert!(
        spans.contains("writer.finish"),
        "expected a writer.finish span; saw: {:?}",
        span_names(&spans)
    );
}

// ---------------------------------------------------------------------------
// Span tree correctness — compaction flow (write-support)
// ---------------------------------------------------------------------------

/// A compaction flow produces the instrumented compaction spans
/// (`compaction.maintenance_step` and, when a merge runs, `compaction.start_merge`).
#[cfg(feature = "write-support")]
#[test]
fn compaction_flow_emits_compaction_spans() {
    ensure_meter_installed();
    let spans = testing::capture_spans(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let mut engine = write_helpers::open_engine(tmp.path());
        engine
            .set_merge_policy(Box::new(write_helpers::policy()))
            .expect("set policy");

        // Three small SSTables so STCS(min_threshold=3) selects them.
        for table in 0..3 {
            for id in 0..5 {
                let pk = table * 100 + id;
                engine
                    .execute(&format!(
                        "INSERT INTO obs_ks.items (id, name, score) VALUES ({pk}, 'n', {pk})"
                    ))
                    .expect("write row");
            }
            rt.block_on(engine.flush())
                .expect("flush")
                .expect("sstable");
        }

        // maintenance_step uses an internal block_on; safe here because we run it
        // on a current-thread runtime via spawn_blocking-free direct call OUTSIDE
        // an active runtime context.
        let budget = std::time::Duration::from_secs(30);
        for _ in 0..5 {
            let report = engine.maintenance_step(budget).expect("maintenance_step");
            if !report.completed_merges.is_empty() || !report.pending_compaction {
                break;
            }
        }
        drop(rt);
    });

    assert!(
        spans.contains("compaction.maintenance_step"),
        "expected a compaction.maintenance_step span; saw: {:?}",
        span_names(&spans)
    );
    assert!(
        spans.contains("compaction.start_merge"),
        "expected a compaction.start_merge span (a merge should have run); saw: {:?}",
        span_names(&spans)
    );
}

// ---------------------------------------------------------------------------
// Metric correctness — names, units, and induced error increment (one serial test)
// ---------------------------------------------------------------------------

/// Catalog metrics are emitted with the right names + units, and
/// `cqlite.errors.total` increments on an induced error with bounded
/// `{category, subsystem}` labels.
#[test]
fn catalog_metrics_have_expected_names_units_and_error_labels() {
    let mc = testing::metrics_capture();

    // --- Phase 0: eager registration at 0 before any error (issue #2288) ---
    // On a freshly-started server `cqlite.errors.total` must be PRESENT at 0, not
    // absent, so "metric name absent" unambiguously means *error counting isn't
    // wired*. Production `otel::init` seeds this baseline once at startup under
    // cumulative temporality (visible in every scrape); this DELTA-temporality
    // harness mirrors that seed with `seed_baseline()` in an isolated collect
    // window with NO error induced, then asserts the series is present at exactly
    // 0. This runs inside the single serial metric test to avoid racing another
    // flow's `errors.total` emission on the process-global provider.
    mc.reset();
    mc.seed_baseline();
    let baseline = mc.flush_and_collect();
    assert!(
        baseline.contains(catalog::ERRORS_TOTAL),
        "cqlite.errors.total must be REGISTERED at startup before any error; saw: {:?}",
        metric_names(&baseline)
    );
    assert_eq!(
        baseline.unit(catalog::ERRORS_TOTAL),
        Some(catalog::unit::ERRORS),
        "eagerly-registered cqlite.errors.total must carry the errors unit"
    );
    // Unlabeled baseline series (no invented {category, subsystem}) at exactly 0,
    // and no other series contributes a nonzero increment in this window.
    assert!(
        mc_baseline_is_zero(&baseline),
        "eagerly-registered cqlite.errors.total baseline must be exactly 0 before any error; \
         saw entries: {:?}",
        baseline.find(catalog::ERRORS_TOTAL)
    );

    // --- Phase 1: names, units, and induced-error labels ---
    mc.reset();

    // Defense-in-depth against cross-test metric bleed: the in-memory exporter is
    // process-global and uses DELTA temporality (so each collect returns only the
    // values recorded since `reset`). To stay correct even if another test's
    // instrumented flow emits into the same DELTA window concurrently, every
    // assertion here keys on a UNIQUE per-test attribute value so it only ever
    // observes ITS OWN time series. The subsystem label below is unique to this
    // test, and the counter/gauge/histogram assertions check presence/units (not
    // exact totals across shared series).
    let unique_subsystem = "obs_correctness_self_test";

    // Emit one of each instrument kind via the production helpers.
    obs::add_counter(
        catalog::READ_ROWS,
        42,
        &[(catalog::attr::SSTABLE_FORMAT, "bti".into())],
    );
    obs::record_histogram(catalog::QUERY_DURATION, 0.005, &[]);
    obs::record_gauge(catalog::SSTABLES_OPEN, 3, &[]);

    // Induce an error so errors.total increments with bounded labels. Tag it with
    // a subsystem value unique to this test so the labeled-sum assertion below
    // matches only this emission, never another flow's `errors.total`.
    let err: Error = Error::corruption("induced for test");
    let expected_category = err.obs_category().as_str();
    obs::record_error(&err, unique_subsystem);

    let metrics = mc.flush_and_collect();

    // Names + units.
    assert!(
        metrics.contains(catalog::READ_ROWS),
        "cqlite.read.rows must be collected; saw: {:?}",
        metric_names(&metrics)
    );
    assert_eq!(
        metrics.unit(catalog::READ_ROWS),
        Some(catalog::unit::ROWS),
        "cqlite.read.rows unit must be {}",
        catalog::unit::ROWS
    );
    assert!(metrics.counter_sum(catalog::READ_ROWS) >= 42.0);

    assert!(metrics.contains(catalog::QUERY_DURATION));
    assert_eq!(
        metrics.unit(catalog::QUERY_DURATION),
        Some(catalog::unit::SECONDS),
        "cqlite.query.duration unit must be seconds"
    );

    assert!(metrics.contains(catalog::SSTABLES_OPEN));
    assert_eq!(
        metrics.unit(catalog::SSTABLES_OPEN),
        Some(catalog::unit::SSTABLES)
    );

    // errors.total with bounded {category, subsystem} labels.
    assert!(metrics.contains(catalog::ERRORS_TOTAL));
    assert_eq!(
        metrics.unit(catalog::ERRORS_TOTAL),
        Some(catalog::unit::ERRORS)
    );
    let labeled = metrics.sum_where(
        catalog::ERRORS_TOTAL,
        &[
            (catalog::attr::ERROR_CATEGORY, expected_category),
            (catalog::attr::SUBSYSTEM, unique_subsystem),
        ],
    );
    // The unique subsystem label means this matches only this test's emission, so
    // the DELTA-window count is exactly one regardless of concurrent flows.
    assert!(
        (labeled - 1.0).abs() < f64::EPSILON,
        "cqlite.errors.total{{category={expected_category},subsystem={unique_subsystem}}} must \
         increment exactly once; saw entries: {:?}",
        metrics.find(catalog::ERRORS_TOTAL)
    );

    // The induced category must be one of the bounded taxonomy values, never a
    // raw message.
    assert!(
        ErrorCategory::ALL
            .iter()
            .any(|c| c.as_str() == expected_category),
        "induced error category {expected_category} must be a bounded taxonomy value"
    );
}

// ---------------------------------------------------------------------------
// Sampling validation (parent-based trace-id-ratio)
// ---------------------------------------------------------------------------

/// `sampling_ratio = 0.0` drops spans; `sampling_ratio = 1.0` keeps them; and a
/// parent-based sampler keeps children of a sampled parent. We validate this at
/// the sampler level (the same `Sampler::ParentBased(TraceIdRatioBased)` the
/// production `init` builds from `ObservabilityConfig.sampling_ratio`) using an
/// in-memory tracer so the result is deterministic.
#[test]
fn sampling_ratio_zero_drops_and_one_keeps() {
    // ratio = 0.0 → root spans dropped.
    let dropped = capture_with_sampler(0.0, || {
        let span = tracing::info_span!("sampled.root");
        let _g = span.enter();
        tracing::info_span!("sampled.child").in_scope(|| {});
    });
    assert!(
        dropped.all().is_empty(),
        "ratio=0.0 must drop all spans; saw: {:?}",
        span_names(&dropped)
    );

    // ratio = 1.0 → spans kept, and child nests under parent.
    let kept = capture_with_sampler(1.0, || {
        let span = tracing::info_span!("sampled.root");
        let _g = span.enter();
        tracing::info_span!("sampled.child").in_scope(|| {});
    });
    assert!(
        kept.contains("sampled.root") && kept.contains("sampled.child"),
        "ratio=1.0 must keep both spans; saw: {:?}",
        span_names(&kept)
    );
    assert!(
        kept.is_parent_of("sampled.root", "sampled.child"),
        "parent-based sampler must keep the child under its sampled parent"
    );
}

/// Hot-path span creation is cheap when not sampled: with ratio=0.0 a large
/// number of spans produces no exported spans (and does not panic / allocate
/// exporter buffers). This is a behavioural smoke test of the "cheap when not
/// sampled" property, not a microbenchmark (the bench covers timing).
#[test]
fn unsampled_hot_path_creates_no_spans() {
    let captured = capture_with_sampler(0.0, || {
        for i in 0..10_000u32 {
            tracing::info_span!("hot.span", i).in_scope(|| {});
        }
    });
    assert!(
        captured.all().is_empty(),
        "ratio=0.0 hot path must export zero spans, got {}",
        captured.all().len()
    );
}

// ---------------------------------------------------------------------------
// Local helpers
// ---------------------------------------------------------------------------

/// Build an in-memory tracer with the SAME sampler the production `init` uses
/// (`ParentBased(TraceIdRatioBased(ratio))`), install it as the thread-local
/// default subscriber, run `flow`, and return the captured spans.
fn capture_with_sampler<F: FnOnce()>(ratio: f64, flow: F) -> testing::CapturedSpans {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use tracing_subscriber::prelude::*;

    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)));
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_sampler(sampler)
        .build();
    let tracer = provider.tracer("cqlite");
    let layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::registry().with(layer);

    tracing::subscriber::with_default(subscriber, flow);
    let _ = provider.force_flush();

    let raw: Vec<(String, opentelemetry_sdk::trace::SpanData)> = exporter
        .get_finished_spans()
        .expect("in-memory spans")
        .into_iter()
        .map(|data| (data.name.to_string(), data))
        .collect();
    let _ = provider.shutdown();
    testing::CapturedSpans::from_raw(raw)
}

fn span_names(spans: &testing::CapturedSpans) -> Vec<String> {
    spans.iter().map(|s| s.name.clone()).collect()
}

// Only used by the cli-helpers read/query span test.
#[cfg(feature = "cli-helpers")]
fn span_tree(spans: &testing::CapturedSpans) -> Vec<(String, String)> {
    spans
        .iter()
        .map(|s| (s.name.clone(), format!("{:?}", s.parent_span_id())))
        .collect()
}

fn metric_names(m: &testing::CapturedMetrics) -> Vec<String> {
    m.entries().iter().map(|e| e.name.clone()).collect()
}

/// The eagerly-seeded `cqlite.errors.total` baseline (issue #2288) is present and
/// totals exactly 0: an actual UNLABELED (empty-attribute) baseline point exists
/// at value 0 AND no series contributes a nonzero increment in the collect window.
///
/// The unlabeled-point check is deliberately NOT `sum_where(.., &[])`, whose empty
/// predicate matches every point vacuously (a zero-valued *labeled* point would
/// pass): `has_point_with_empty_attrs_at` requires a genuinely attribute-free
/// baseline point, so this fails if the seeded point were labeled or absent.
fn mc_baseline_is_zero(m: &testing::CapturedMetrics) -> bool {
    m.has_point_with_empty_attrs_at(catalog::ERRORS_TOTAL, 0.0)
        && m.counter_sum(catalog::ERRORS_TOTAL).abs() < f64::EPSILON
}

// ---------------------------------------------------------------------------
// Read fixtures (mirrors benches/fixtures, trimmed to what these tests need)
// ---------------------------------------------------------------------------

#[cfg(feature = "cli-helpers")]
mod read_fixtures {
    use std::path::PathBuf;

    pub struct ReadFixture {
        pub keyspace: &'static str,
        pub table: &'static str,
        pub schema_file: &'static str,
    }

    impl ReadFixture {
        pub fn qualified(&self) -> String {
            format!("{}.{}", self.keyspace, self.table)
        }
    }

    pub const SIMPLE: ReadFixture = ReadFixture {
        keyspace: "test_basic",
        table: "simple_table",
        schema_file: "basic-types.cql",
    };

    pub struct ReadDb {
        pub db: cqlite_core::Database,
        _tmp: tempfile::TempDir,
    }

    fn datasets_root() -> PathBuf {
        match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets"),
        }
    }

    fn table_dir(keyspace: &str, table: &str) -> PathBuf {
        let parent = datasets_root().join("sstables").join(keyspace);
        let prefix = format!("{table}-");
        std::fs::read_dir(&parent)
            .unwrap_or_else(|e| panic!("read fixture dir {}: {e}", parent.display()))
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with(&prefix))
            .map(|e| e.path())
            .unwrap_or_else(|| {
                panic!(
                    "fixture {keyspace}/{table} not found under {} — fetch datasets first",
                    parent.display()
                )
            })
    }

    fn copy_dir(src: &std::path::Path, dst: &std::path::Path) {
        std::fs::create_dir_all(dst).expect("create dst");
        for entry in std::fs::read_dir(src).expect("read src") {
            let entry = entry.expect("dir entry");
            let from = entry.path();
            let to = dst.join(entry.file_name());
            if from.is_dir() {
                copy_dir(&from, &to);
            } else {
                std::fs::copy(&from, &to).expect("copy file");
            }
        }
    }

    pub fn open_read_db(fx: &ReadFixture) -> ReadDb {
        use cqlite_core::ingestion::{ingest, IngestionConfig};

        let src = table_dir(fx.keyspace, fx.table);
        let tmp = tempfile::TempDir::new().expect("temp dir");
        let dst = tmp
            .path()
            .join(fx.keyspace)
            .join(src.file_name().expect("dir name"));
        copy_dir(&src, &dst);

        let schema_path = datasets_root().join("../schemas").join(fx.schema_file);
        let cfg = IngestionConfig {
            schema_paths: vec![schema_path],
            data_dir: tmp.path().to_path_buf(),
            version_hint: Some("5.0".to_string()),
            core_config: cqlite_core::Config::default(),
            table_directory_filter: Some(format!("/{}/{}", fx.keyspace, fx.table)),
        };

        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let db = rt.block_on(ingest(cfg)).expect("ingest").database;
        ReadDb { db, _tmp: tmp }
    }
}

// ---------------------------------------------------------------------------
// Write helpers
// ---------------------------------------------------------------------------

#[cfg(feature = "write-support")]
mod write_helpers {
    use cqlite_core::schema::{Column, KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::{STCSPolicy, WriteEngine, WriteEngineConfig};
    use std::collections::HashMap;

    fn schema() -> TableSchema {
        TableSchema {
            keyspace: "obs_ks".to_string(),
            table: "items".to_string(),
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
                Column {
                    name: "score".to_string(),
                    data_type: "int".to_string(),
                    nullable: true,
                    default: None,
                    is_static: false,
                },
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    pub fn open_engine(dir: &std::path::Path) -> WriteEngine {
        let cfg = WriteEngineConfig::new(dir.join("data"), dir.join("wal"), schema());
        WriteEngine::new(cfg).expect("build write engine")
    }

    pub fn policy() -> STCSPolicy {
        STCSPolicy::new(3, 32, 0.5, 1.5, 0).expect("valid STCS policy")
    }
}
