//! Read-path metric honesty: the four headline READ metrics must actually be
//! emitted by the read path (issue #1701, epic #1686 observability honesty).
//!
//! # What this pins, and why it is a HONESTY test
//!
//! `cqlite.read.rows`, `cqlite.read.bytes`, `cqlite.read.partitions` and
//! `cqlite.read.duration` are documented in
//! [`cqlite_core::observability::catalog`], registered as instruments, rendered in
//! the operator metric reference, and showcased in the observability module's own
//! doc example — and before this issue NO production read path ever updated them.
//! An operator running a full `observability` build could not see read throughput
//! at all. A metric that is documented but never written is worse than an absent
//! one: a dashboard shows a flat zero and reads as "no reads happening".
//!
//! So the assertions here are deliberately about EMISSION AT A REAL READ SURFACE
//! (`SSTableReader::scan_stream`, `scan_stream_batched`, `get`) over REAL Cassandra
//! 5.0 SSTables — never about a helper being callable.
//!
//! # Granularity contract (why totals, not per-row assertions)
//!
//! The emission is at BATCH granularity: one counter add and one duration record
//! per read OPERATION (per chunk for `read.bytes`), never per row. The assertions
//! therefore compare the operation TOTAL against the rows the test itself received
//! — an independent tally — and require the duration histogram to hold at least one
//! recording. There is deliberately NO wall-clock threshold assertion anywhere
//! here: "a recording exists" is the property; "it took less than X ms" would be a
//! flake (#2642).
//!
//! # Fixture contract (#3220)
//!
//! Both fixtures are COMMITTED to git (`test_big/wide_partition`, a compressed BIG
//! `nb` SSTable with 3 partitions / 892 on-disk rows, and its Index.db sibling), so
//! they can never be legitimately absent in any checkout: resolution failure is a
//! hard FAILURE, per case, unconditionally — never a suite-wide `assert!(ran > 0)`
//! and never a silent SKIP. A present-but-empty scan (0 rows) fails too.
//!
//! # Why every metric assertion lives in ONE serial test
//!
//! The production metric helpers record through a single process-global `Meter`
//! that binds on first use, so the in-memory capture provider is process-wide and
//! cannot be swapped per test; the exporter uses DELTA temporality, so a
//! concurrently-running sibling test's read flow would land in this test's collect
//! window and break an exact-total assertion. The phases below therefore run in one
//! test, each `reset()`-ing immediately before its own flow — the same rationale
//! (and the same shape) as `observability_correctness.rs`.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core --features observability-testing \
//!   --test observability_read_metrics
//! ```

#![cfg(feature = "observability-testing")]

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use cqlite_core::observability::catalog;
use cqlite_core::observability::testing::{self, CapturedMetrics};
use cqlite_core::storage::sstable::SSTableReader;
use cqlite_core::{Config, Platform, RowKey, ScanRow, TableId};

#[path = "support/datasets_root.rs"]
mod datasets_root;

/// A COMMITTED fixture, its `catalog::attr::SSTABLE_FORMAT` label, and the shape
/// that makes it worth reading here.
struct Fixture {
    keyspace: &'static str,
    table: &'static str,
    /// The bounded format label the emission must carry for this SSTable.
    format: &'static str,
    /// Whether a SECOND scan of this SSTable is served from the resident
    /// decompressed-chunk cache and therefore reads NO `Data.db` bytes.
    ///
    /// True for the BIG windowed scan plane, whose decode goes through the B1
    /// decompressed-chunk cache (`ChunkSource::decode_borrowed`). FALSE for the BTI
    /// whole-file trie walk, which decompresses UNCACHED
    /// (`ChunkSource::decompress_only`): its re-scan genuinely re-reads and
    /// re-decompresses every chunk, so `read.bytes` must count them AGAIN. The
    /// metric reports the I/O that happened — hiding a real re-read would be exactly
    /// the dishonesty this issue removes.
    warm_scan_is_cached: bool,
    /// Partitions this fixture must deliver, at minimum. Two or more is what makes
    /// `read.partitions` distinguishable from `read.rows`; the single-partition
    /// UNCOMPRESSED fixture below is here for the byte-counting direction instead
    /// (it is the only committed uncompressed SSTable).
    min_partitions: u64,
    /// The bounded `catalog::attr::COMPRESSION` label the byte counter must carry —
    /// `"none"` for an SSTable with no `CompressionInfo.db`.
    compression: &'static str,
}

/// A COMMITTED compressed BIG (`nb`) fixture: 3 partitions, 892 on-disk rows, with
/// a `CompressionInfo.db` so the scan really decompresses chunks (which is what
/// `read.bytes` counts).
const BIG: Fixture = Fixture {
    keyspace: "test_big",
    table: "wide_partition",
    format: "big",
    warm_scan_is_cached: true,
    min_partitions: 2,
    compression: "lz4",
};

/// A COMMITTED BTI (`da`) fixture: 3 partitions, 900 on-disk rows. The BTI trie walk
/// is a SEPARATE decode path from the BIG windowed scan, and it carries a different
/// bounded format label, so both are exercised.
const BTI: Fixture = Fixture {
    keyspace: "test_da",
    table: "wide_table",
    format: "bti",
    warm_scan_is_cached: false,
    min_partitions: 2,
    compression: "lz4",
};

/// A COMMITTED **UNCOMPRESSED** BIG (`nb`) fixture (no `CompressionInfo.db`): 1
/// partition, 600 on-disk rows.
///
/// This case exists for roborev B2. Uncompressed is a FIRST-CLASS read path — the
/// #1406 claim boundary says CQLite's own production write surface emits
/// uncompressed SSTables ONLY — and the byte counter used to skip the
/// no-compressor decode branch entirely, so every uncompressed read was invisible.
/// Both compressed fixtures above are blind to that by construction, which is why a
/// third fixture is needed rather than another assertion on the first two.
const UNCOMPRESSED: Fixture = Fixture {
    keyspace: "test_comp",
    table: "uncompressed_table",
    format: "big",
    // The uncompressed windowed exit MOVES the read buffer into the B1
    // decompressed-chunk cache (zero-copy, issue #1940 BLOCKER-1), so a re-scan is a
    // cache hit and reads no Data.db bytes — same as the compressed BIG plane.
    warm_scan_is_cached: true,
    min_partitions: 1,
    compression: "none",
};

/// Resolve a fixture's `Data.db`. FAIL-CLOSED: the fixture is committed, so an
/// absence is a resolution defect, never a legitimate skip (#3220).
fn committed_data_db(fx: &Fixture) -> PathBuf {
    let (keyspace, table) = (fx.keyspace, fx.table);
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

async fn open_reader(fx: &Fixture) -> Arc<SSTableReader> {
    let data_db = committed_data_db(fx);
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

/// Rows + distinct partition keys the TEST observed, computed independently of the
/// instrumentation: `partitions` is a set of distinct keys, while the production
/// accounting counts partition TRANSITIONS in scan order — two different
/// computations, so the equality below has teeth.
struct Tally {
    rows: u64,
    partitions: u64,
}

fn tally(entries: &[(RowKey, ScanRow)]) -> Tally {
    let distinct: HashSet<&[u8]> = entries.iter().map(|(k, _)| k.as_bytes()).collect();
    Tally {
        rows: entries.len() as u64,
        partitions: distinct.len() as u64,
    }
}

/// Number of recordings aggregated into a histogram metric (0 when absent).
fn histogram_recordings(metrics: &CapturedMetrics, name: &str) -> u64 {
    metrics
        .find(name)
        .map(|m| m.points.iter().filter_map(|p| p.count).sum())
        .unwrap_or(0)
}

fn metric_names(metrics: &CapturedMetrics) -> Vec<String> {
    metrics.entries().iter().map(|m| m.name.clone()).collect()
}

/// Assert the four READ metrics for one completed read operation.
///
/// `format` is the bounded `catalog::attr::SSTABLE_FORMAT` value the emission must
/// carry: a single-SSTable read knows its format, so the label must be THERE and be
/// the RIGHT one — the row/partition totals are therefore asserted BOTH as the
/// metric-wide sum and as the sum of the labelled series, so an unlabelled (or
/// mislabelled) emission cannot satisfy them.
fn assert_read_metrics(
    metrics: &CapturedMetrics,
    phase: &str,
    expected: &Tally,
    fx: &Fixture,
    expect_bytes: bool,
) {
    let format = fx.format;
    let labelled = [(catalog::attr::SSTABLE_FORMAT, format)];
    assert_eq!(
        metrics.sum_where(catalog::READ_ROWS, &labelled),
        expected.rows as f64,
        "[{phase}] cqlite.read.rows must carry the bounded {} = {format} label a \
         single-SSTable read knows; collected metrics: {:?}, points: {:?}",
        catalog::attr::SSTABLE_FORMAT,
        metric_names(metrics),
        metrics.find(catalog::READ_ROWS).map(|m| &m.points)
    );
    assert_eq!(
        metrics.sum_where(catalog::READ_PARTITIONS, &labelled),
        expected.partitions as f64,
        "[{phase}] cqlite.read.partitions must carry the bounded {} = {format} \
         label; points: {:?}",
        catalog::attr::SSTABLE_FORMAT,
        metrics.find(catalog::READ_PARTITIONS).map(|m| &m.points)
    );
    assert_eq!(
        metrics.counter_sum(catalog::READ_ROWS),
        expected.rows as f64,
        "[{phase}] cqlite.read.rows must equal the {} rows the read actually \
         delivered; collected metrics: {:?}",
        expected.rows,
        metric_names(metrics)
    );
    assert_eq!(
        metrics.unit(catalog::READ_ROWS),
        Some(catalog::unit::ROWS),
        "[{phase}] cqlite.read.rows unit"
    );

    assert_eq!(
        metrics.counter_sum(catalog::READ_PARTITIONS),
        expected.partitions as f64,
        "[{phase}] cqlite.read.partitions must equal the {} distinct partitions the \
         read touched; collected metrics: {:?}",
        expected.partitions,
        metric_names(metrics)
    );
    assert_eq!(
        metrics.unit(catalog::READ_PARTITIONS),
        Some(catalog::unit::PARTITIONS),
        "[{phase}] cqlite.read.partitions unit"
    );

    // One duration RECORDING per read operation — never a wall-clock threshold
    // (#2642): the property is that the operation was timed at all.
    assert!(
        histogram_recordings(metrics, catalog::READ_DURATION) >= 1,
        "[{phase}] cqlite.read.duration must hold at least one recording for a \
         completed read operation; collected metrics: {:?}",
        metric_names(metrics)
    );
    assert_eq!(
        metrics.unit(catalog::READ_DURATION),
        Some(catalog::unit::SECONDS),
        "[{phase}] cqlite.read.duration unit"
    );

    if expect_bytes {
        assert!(
            metrics.counter_sum(catalog::READ_BYTES) > 0.0,
            "[{phase}] cqlite.read.bytes must count the Data.db bytes the read \
             materialised; collected metrics: {:?}",
            metric_names(metrics)
        );
        assert_eq!(
            metrics.unit(catalog::READ_BYTES),
            Some(catalog::unit::BYTES),
            "[{phase}] cqlite.read.bytes unit"
        );
        // The byte counter's ONE documented attribute (issue #1701 roborev B3: the
        // chunk decode plane knows the compressor, not the SSTable format). For an
        // uncompressed SSTable that value is the bounded `"none"`, not an absent
        // label — the regression B2 fixed made those reads vanish entirely, and an
        // unlabelled emission would satisfy a bare `> 0` check.
        let byte_labelled = [(catalog::attr::COMPRESSION, fx.compression)];
        assert_eq!(
            metrics.sum_where(catalog::READ_BYTES, &byte_labelled),
            metrics.counter_sum(catalog::READ_BYTES),
            "[{phase}] every cqlite.read.bytes point must carry {} = {}; points: {:?}",
            catalog::attr::COMPRESSION,
            fx.compression,
            metrics.find(catalog::READ_BYTES).map(|m| &m.points)
        );
        assert!(
            metrics
                .find(catalog::READ_BYTES)
                .is_some_and(|m| m.points.iter().all(|p| p.attributes.len() == 1)),
            "[{phase}] cqlite.read.bytes must carry EXACTLY its one documented \
             attribute — no format label the chunk plane cannot honestly know; \
             points: {:?}",
            metrics.find(catalog::READ_BYTES).map(|m| &m.points)
        );
    }
}

/// Drive the three read surfaces of ONE SSTable and assert the four metrics after
/// each. Shared by the BIG and BTI cases so both formats are held to the identical
/// contract (the emission differs only in the bounded format label).
async fn exercise_read_surfaces(fx: &Fixture, mc: &testing::MetricsCapture) {
    let reader = open_reader(fx).await;
    let tid = TableId::new(format!("{}.{}", fx.keyspace, fx.table));

    // ---------------------------------------------------------------------
    // Phase 1 — the per-row streaming scan surface (`scan_stream`). COLD: this is
    // the first read of this SSTable, so its chunks are decompressed for real and
    // `read.bytes` must be counted.
    // ---------------------------------------------------------------------
    mc.reset();
    let mut entries = Vec::new();
    {
        let mut stream = reader
            .clone()
            .scan_stream(tid.clone(), None, None, None, 64);
        while let Some(item) = stream.recv().await {
            entries.push(item.expect("per-row scan stream item"));
        }
    }
    let scan_metrics = mc.flush_and_collect();
    let scan_tally = tally(&entries);
    assert!(
        scan_tally.rows > 0,
        "[{}.{}] the committed fixture is present but the scan delivered 0 rows — a \
         dataset-dependent assertion must never pass on an empty read",
        fx.keyspace,
        fx.table
    );
    assert!(
        scan_tally.partitions >= fx.min_partitions,
        "[{}.{}] the fixture must deliver at least {} partition(s) for this case to \
         mean anything (saw {})",
        fx.keyspace,
        fx.table,
        fx.min_partitions,
        scan_tally.partitions
    );
    assert_read_metrics(&scan_metrics, "scan_stream", &scan_tally, fx, true);

    // ---------------------------------------------------------------------
    // Phase 2 — the BATCHED streaming scan surface (`scan_stream_batched`), the
    // one the streaming SELECT executor consumes.
    // ---------------------------------------------------------------------
    mc.reset();
    let mut batched_entries = Vec::new();
    {
        let mut stream = reader
            .clone()
            .scan_stream_batched(tid.clone(), None, None, None, 64);
        while let Some(item) = stream.recv().await {
            batched_entries.extend(item.expect("batched scan stream item"));
        }
    }
    let batched_metrics = mc.flush_and_collect();
    let batched_tally = tally(&batched_entries);
    assert_eq!(
        batched_tally.rows, scan_tally.rows,
        "[{}.{}] the batched surface must deliver the same rows as the per-row one",
        fx.keyspace, fx.table
    );
    assert_read_metrics(
        &batched_metrics,
        "scan_stream_batched",
        &batched_tally,
        fx,
        !fx.warm_scan_is_cached,
    );
    // ... and the read.bytes semantic in BOTH directions, pinned positively per
    // decode plane: "bytes read from Data.db", never "bytes handed to the decoder".
    if fx.warm_scan_is_cached {
        assert_eq!(
            batched_metrics.counter_sum(catalog::READ_BYTES),
            0.0,
            "[{}.{}] a warm scan on the CACHED decode plane must count ZERO \
             cqlite.read.bytes — counting a cache hit would overstate the Data.db \
             I/O the read performed",
            fx.keyspace,
            fx.table
        );
    } else {
        assert!(
            batched_metrics.counter_sum(catalog::READ_BYTES) > 0.0,
            "[{}.{}] a re-scan on the UNCACHED decode plane really re-reads and \
             re-decompresses every chunk, so cqlite.read.bytes must count them \
             again — reporting zero would hide real I/O",
            fx.keyspace,
            fx.table
        );
    }

    // ---------------------------------------------------------------------
    // Phase 3 — the point read (`get`). One partition, one row.
    // ---------------------------------------------------------------------
    let key = entries
        .first()
        .map(|(k, _)| k.clone())
        .expect("a present partition key learned from the scan");
    mc.reset();
    let row = reader.get(&tid, &key).await.expect("point read");
    let point_metrics = mc.flush_and_collect();
    assert!(row.is_some(), "the learned key must resolve a row");
    assert_read_metrics(
        &point_metrics,
        "get",
        &Tally {
            rows: 1,
            partitions: 1,
        },
        fx,
        // Bytes are counted per decompressed chunk; the point read serves its
        // covering chunk from the resident chunk cache populated by the scans
        // above, and a cache hit reads no Data.db bytes. So the point phase does
        // not assert bytes — the cold scan phase does.
        false,
    );
}

// Serialized against the sibling case below: the capture harness's meter provider is
// process-global with DELTA temporality, so two metric-asserting tests running
// concurrently would land in each other's collect window and break an exact total.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn read_path_emits_rows_bytes_partitions_and_duration() {
    let mc = testing::metrics_capture();
    // BIG (`nb`) windowed scan plane and BTI (`da`) trie walk are separate decode
    // paths carrying different bounded format labels; both must report.
    exercise_read_surfaces(&BIG, &mc).await;
    exercise_read_surfaces(&BTI, &mc).await;
    // UNCOMPRESSED (roborev B2): the no-compressor decode branch is a real read of
    // Data.db payload and must be counted, under the bounded `compression = "none"`
    // label. Neither fixture above can see this — both are compressed.
    exercise_read_surfaces(&UNCOMPRESSED, &mc).await;
}

// ---------------------------------------------------------------------------
// Cross-generation point read: ONE logical read is ONE metered operation
// (issue #1701, roborev B1)
// ---------------------------------------------------------------------------

/// A logical point read may probe several SSTable generations of one table.
/// `SSTableManager::get` used to call the METERED per-reader lookup, so one `get`
/// emitted one `cqlite.read.duration` sample PER CANDIDATE and counted a row once per
/// matching generation instead of once per reconciled result — a metric overstating
/// both the read rate and the read count.
///
/// # Why this fixture is CQLite-written, deliberately
///
/// No COMMITTED corpus table carries more than one generation (the multi-generation
/// tables in the fetched corpus are not in the checkout), so the only fixture that is
/// guaranteed present in EVERY checkout is one this test builds. That is sound here
/// because the property under test is METRIC ACCOUNTING over a known number of
/// generations, not an on-disk framing/encoding property — the case #3042 reserves
/// for Cassandra-written bytes. A CQLite-written SSTable cannot make this assertion
/// pass vacuously: the generation COUNT is verified on disk, and the row/duration
/// counts come from the production emission path either way.
#[cfg(feature = "write-support")]
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(read_metrics)]
async fn a_cross_generation_point_read_is_one_metered_operation() {
    use cqlite_core::schema::parse_cql_schema;
    use cqlite_core::storage::sstable::SSTableManager;
    use cqlite_core::storage::write_engine::{
        CellOperation, Mutation, PartitionKey, WriteEngine, WriteEngineConfig,
    };
    use cqlite_core::types::Value;

    const KS: &str = "read_metrics_ks";
    const TBL: &str = "items";

    let mc = testing::metrics_capture();
    let tmp = tempfile::TempDir::new().expect("temp dir");
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    std::fs::create_dir_all(&wal_dir).expect("create wal dir");

    let schema_cql = format!("CREATE TABLE {KS}.{TBL} (\n  id int PRIMARY KEY,\n  name text\n);\n");
    let schema = parse_cql_schema(&schema_cql).expect("parse fixture schema");

    let write_row = |id: i32| {
        Mutation::new(
            cqlite_core::storage::write_engine::TableId::new(KS, TBL),
            PartitionKey::single("id", Value::Integer(id)),
            None,
            vec![CellOperation::Write {
                column: "name".to_string(),
                value: Value::text(format!("row-{id}")),
            }],
            100 + id as i64,
            None,
        )
    };

    // Two flushes, no compaction => two generations the point read must walk.
    {
        let config = WriteEngineConfig::new(data_dir.clone(), wal_dir.clone(), schema.clone());
        let mut engine = WriteEngine::new(config).expect("write engine");
        for id in 1..=4 {
            engine.write(write_row(id)).expect("write gen1 row");
        }
        engine
            .flush()
            .await
            .expect("flush gen1")
            .expect("gen1 produced no SSTable");
        for id in 5..=8 {
            engine.write(write_row(id)).expect("write gen2 row");
        }
        engine
            .flush()
            .await
            .expect("flush gen2")
            .expect("gen2 produced no SSTable");
        engine.close().await.expect("close engine");
    }

    let sstable_dir = data_dir.join(KS).join(TBL);
    let generations = std::fs::read_dir(&sstable_dir)
        .expect("read sstable dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
        .count();
    assert_eq!(
        generations,
        2,
        "the fixture must hold exactly 2 generations for this case to say anything \
         about per-generation double counting (saw {generations} in {})",
        sstable_dir.display()
    );

    let config = Config::default();
    let platform = Arc::new(Platform::new(&config).await.expect("platform"));
    let manager = SSTableManager::new(
        &data_dir,
        &config,
        platform,
        #[cfg(feature = "state_machine")]
        None,
    )
    .await
    .expect("open the two-generation table");
    let tid = TableId::new(format!("{KS}.{TBL}"));

    // Learn a present partition key through the public scan surface (outside any
    // collect window, so it contributes no metrics to the assertions below).
    let scanned = manager
        .scan(&tid, None, None, None, Some(&schema))
        .await
        .expect("scan the two-generation table");
    assert!(
        scanned.len() >= 8,
        "both generations' rows must be readable before metering is asserted (saw {})",
        scanned.len()
    );
    let present_key = scanned
        .last()
        .map(|(k, _)| k.clone())
        .expect("a present partition key");

    // --- A PRESENT key: one operation, one row, one partition ----------------
    mc.reset();
    let row = manager
        .get(&tid, &present_key)
        .await
        .expect("cross-generation point read");
    let hit = mc.flush_and_collect();
    assert!(row.is_some(), "the learned key must resolve a row");
    assert_eq!(
        histogram_recordings(&hit, catalog::READ_DURATION),
        1,
        "ONE logical point read must record EXACTLY ONE cqlite.read.duration sample, \
         whatever the number of generations it probed; points: {:?}",
        hit.find(catalog::READ_DURATION).map(|m| &m.points)
    );
    assert_eq!(
        hit.counter_sum(catalog::READ_ROWS),
        1.0,
        "the RECONCILED result is one row, even when several generations match; \
         points: {:?}",
        hit.find(catalog::READ_ROWS).map(|m| &m.points)
    );
    assert_eq!(
        hit.counter_sum(catalog::READ_PARTITIONS),
        1.0,
        "one partition; points: {:?}",
        hit.find(catalog::READ_PARTITIONS).map(|m| &m.points)
    );
    // Format-agnostic at this grain: the generations of one table need not share an
    // on-disk format, so the emission must carry NO `sstable.format` label rather
    // than an arbitrarily-picked one.
    for name in [
        catalog::READ_ROWS,
        catalog::READ_PARTITIONS,
        catalog::READ_DURATION,
    ] {
        let points = hit.find(name).map(|m| m.points.clone()).unwrap_or_default();
        assert!(
            !points.is_empty() && points.iter().all(|p| p.attributes.is_empty()),
            "{name} from a CROSS-GENERATION read must carry no attributes (a single \
             format label would be a fabrication); points: {points:?}"
        );
    }

    // --- An ABSENT key: still exactly ONE operation ---------------------------
    // This is the deterministic shape of the defect: absence probes EVERY generation,
    // so the pre-fix code emitted one duration sample per candidate (2 here) for one
    // logical read. A read that resolves absence still reports its latency, with zero
    // rows — dropping it would bias the distribution toward hits.
    let absent_key = RowKey::new(987_654_321i32.to_be_bytes().to_vec());
    mc.reset();
    let missing = manager
        .get(&tid, &absent_key)
        .await
        .expect("absent-key point read");
    let miss = mc.flush_and_collect();
    assert!(missing.is_none(), "the crafted key must be absent");
    assert_eq!(
        histogram_recordings(&miss, catalog::READ_DURATION),
        1,
        "an ABSENT-key read across 2 generations is still ONE operation and must \
         record exactly ONE duration sample; points: {:?}",
        miss.find(catalog::READ_DURATION).map(|m| &m.points)
    );
    assert_eq!(
        miss.counter_sum(catalog::READ_ROWS),
        0.0,
        "an absent key delivered no rows"
    );
    assert_eq!(
        miss.counter_sum(catalog::READ_PARTITIONS),
        0.0,
        "an absent key touched no partition"
    );
}
