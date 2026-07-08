//! Correctness / silent-miss observability signals (issue #2163).
//!
//! Scenario coverage for the `correctness-signals` OpenSpec change. Every metric
//! assertion drives a REAL read / merge / query through a public surface (a
//! compaction via `WriteEngine::maintenance_step`, a point read via
//! `SSTableReader`, a `SELECT` via `Database`) and asserts against the emitted
//! catalog metrics captured by the shared in-memory OTLP exporter.
//!
//! # Why one serial metric test
//!
//! The production metric helpers record through a single process-global `Meter`
//! bound on first use, so the in-memory meter provider is process-wide and uses
//! DELTA temporality. Several of the #2163 counters (`cqlite.merge.rows_in`,
//! `tombstones_suppressed`, …) carry NO disambiguating attribute, so ALL metric
//! assertions here run in ONE serial test that `reset`s the capture immediately
//! before each flow and `flush_and_collect`s immediately after — the same
//! discipline `observability_correctness.rs` documents.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core \
//!   --features observability-testing,cli-helpers,write-support \
//!   --test correctness_signals_2163
//! ```

#![cfg(all(feature = "observability-testing", feature = "write-support"))]

use cqlite_core::observability::{self as obs, catalog, testing};
use cqlite_core::storage::sstable::reader::presence_verification;

// ---------------------------------------------------------------------------
// Requirement: Catalog integrity — every added counter resolves to a
// registered instrument (its declared unit), not the ad-hoc `_ =>` fallback.
// ---------------------------------------------------------------------------

fn assert_registered_unit(metrics: &testing::CapturedMetrics, name: &str, unit: &str) {
    assert!(
        metrics.contains(name),
        "{name} must be collected; saw: {:?}",
        metrics
            .entries()
            .iter()
            .map(|e| e.name.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        metrics.unit(name),
        Some(unit),
        "{name} must carry its registered unit {unit} (proves it hit the pre-registered \
         instrument, not the ad-hoc fallback arm)"
    );
}

// ---------------------------------------------------------------------------
// The single serial metric test.
// ---------------------------------------------------------------------------

#[test]
fn correctness_signals_end_to_end() {
    let mc = testing::metrics_capture();

    // --- Catalog integrity: every added counter dispatches to its instrument ---
    mc.reset();
    obs::add_counter(catalog::MERGE_ROWS_IN, 1, &[]);
    obs::add_counter(catalog::MERGE_ROWS_OUT, 1, &[]);
    obs::add_counter(catalog::COMPACTION_TOMBSTONES_SUPPRESSED, 1, &[]);
    obs::add_counter(catalog::COMPACTION_TOMBSTONES_EMITTED, 1, &[]);
    obs::add_counter(
        catalog::READ_SSTABLES_PRUNED,
        1,
        &[(catalog::attr::SSTABLE_FORMAT, "big".into())],
    );
    obs::add_counter(
        catalog::READ_BLOOM_FALSE_NEGATIVES,
        1,
        &[(catalog::attr::SSTABLE_FORMAT, "big".into())],
    );
    obs::add_counter(
        catalog::QUERY_DEGRADED_PATH,
        1,
        &[(catalog::attr::FALLBACK_REASON, "no_schema".into())],
    );
    let m = mc.flush_and_collect();
    assert_registered_unit(&m, catalog::MERGE_ROWS_IN, catalog::unit::ROWS);
    assert_registered_unit(&m, catalog::MERGE_ROWS_OUT, catalog::unit::ROWS);
    assert_registered_unit(
        &m,
        catalog::COMPACTION_TOMBSTONES_SUPPRESSED,
        catalog::unit::TOMBSTONES,
    );
    assert_registered_unit(
        &m,
        catalog::COMPACTION_TOMBSTONES_EMITTED,
        catalog::unit::TOMBSTONES,
    );
    assert_registered_unit(&m, catalog::READ_SSTABLES_PRUNED, catalog::unit::SSTABLES);
    assert_registered_unit(
        &m,
        catalog::READ_BLOOM_FALSE_NEGATIVES,
        catalog::unit::DIMENSIONLESS,
    );
    assert_registered_unit(
        &m,
        catalog::QUERY_DEGRADED_PATH,
        catalog::unit::DIMENSIONLESS,
    );

    // --- Requirement: Merge row-count reconciliation counters ---
    // Two overlapping generations: partition id=1 appears in BOTH SSTables and
    // LWW-collapses (2 in -> 1 out); id=2 and id=3 pass through. So the reconcile
    // boundary sees N=4 rows in and M=3 out, delta=1 = the collapsed duplicate.
    {
        let mut flows = merge::run_overlapping_merge();
        mc.reset();
        merge::compact(&mut flows);
        let m = mc.flush_and_collect();
        let rows_in = m.counter_sum(catalog::MERGE_ROWS_IN);
        let rows_out = m.counter_sum(catalog::MERGE_ROWS_OUT);
        assert_eq!(
            rows_in,
            4.0,
            "cqlite.merge.rows_in must equal the 4 rows consumed at reconcile; entry: {:?}",
            m.find(catalog::MERGE_ROWS_IN)
        );
        assert_eq!(
            rows_out,
            3.0,
            "cqlite.merge.rows_out must equal the 3 reconciled rows; entry: {:?}",
            m.find(catalog::MERGE_ROWS_OUT)
        );
        assert_eq!(
            rows_in - rows_out,
            1.0,
            "the in/out delta must equal the 1 row removed by reconciliation (LWW collapse)"
        );
    }

    // --- Requirement: Tombstone suppression-vs-emission counters ---
    // A newer whole-row tombstone shadows an older live cell in the same
    // clustering slot: the live cell is SUPPRESSED and the retained tombstone
    // marker is EMITTED, both independently of tombstones_purged.
    {
        let mut flows = merge::run_row_tombstone_merge();
        mc.reset();
        merge::compact(&mut flows);
        let m = mc.flush_and_collect();
        let suppressed = m.counter_sum(catalog::COMPACTION_TOMBSTONES_SUPPRESSED);
        let emitted = m.counter_sum(catalog::COMPACTION_TOMBSTONES_EMITTED);
        assert!(
            suppressed >= 1.0,
            "a row tombstone shadowing an older live cell must count >=1 suppressed; entry: {:?}",
            m.find(catalog::COMPACTION_TOMBSTONES_SUPPRESSED)
        );
        assert!(
            emitted >= 1.0,
            "the retained row-tombstone marker must count >=1 emitted; entry: {:?}",
            m.find(catalog::COMPACTION_TOMBSTONES_EMITTED)
        );
    }

    // --- Requirement: SSTable-pruned-by-presence-oracle counter ---
    // A BIG (nb) SSTable with a bloom filter: probing definitely-absent keys makes
    // `might_contain_partition` return false, pruning the SSTable per probe.
    {
        let reader = fixtures::open_big_reader();
        // `might_contain_partition` is synchronous — no runtime needed here.
        // Count how many absent-key probes the oracle reports definitely-absent.
        mc.reset();
        let mut expected_pruned = 0u64;
        for i in 0..64u32 {
            // 8-byte keys that do not exist in the (int-keyed) fixture table.
            let key = [
                0xFEu8,
                0xED,
                0xFA,
                0xCE,
                (i >> 8) as u8,
                i as u8,
                0xAB,
                0xCD,
            ];
            if !reader.might_contain_partition(&key) {
                expected_pruned += 1;
            }
        }
        let m = mc.flush_and_collect();
        assert!(
            expected_pruned >= 1,
            "with 64 absent-key probes the bloom filter must report >=1 definitive miss"
        );
        assert_eq!(
            m.counter_sum(catalog::READ_SSTABLES_PRUNED),
            expected_pruned as f64,
            "cqlite.read.sstables_pruned must increment once per definitely-absent probe"
        );
        assert_eq!(
            m.sum_where(
                catalog::READ_SSTABLES_PRUNED,
                &[(catalog::attr::SSTABLE_FORMAT, "big")],
            ),
            expected_pruned as f64,
            "every prune must carry cqlite.sstable.format=big for a BIG fixture"
        );
    }

    // --- Requirement: Opt-in presence-oracle false-negative verification ---
    {
        let (reader, table_id, present_key, _tmp) = fixtures::open_writer_reader_with_present_key();
        let rt = tokio::runtime::Runtime::new().expect("rt");
        let absent_key = 0x7FFF_FFFFi32.to_be_bytes();

        // (a) Verification OFF (default): a point read for an absent key performs no
        // confirmation scan and never emits the false-negative counter.
        presence_verification::set_enabled_for_testing(false);
        mc.reset();
        let got = rt
            .block_on(reader.verify_presence_oracle_negative(&table_id, &absent_key))
            .expect("verify off");
        assert!(!got, "verification OFF must not run a confirmation scan");
        let m = mc.flush_and_collect();
        assert_eq!(
            m.counter_sum(catalog::READ_BLOOM_FALSE_NEGATIVES),
            0.0,
            "verification OFF must never emit false_negatives"
        );

        // (b) Verification ON, a genuinely absent key: the authoritative scan runs,
        // finds the key absent, and the counter stays 0.
        presence_verification::set_enabled_for_testing(true);
        mc.reset();
        let got = rt
            .block_on(reader.verify_presence_oracle_negative(&table_id, &absent_key))
            .expect("verify on true-negative");
        assert!(!got, "a genuinely absent key must not be a false negative");
        let m = mc.flush_and_collect();
        assert_eq!(
            m.counter_sum(catalog::READ_BLOOM_FALSE_NEGATIVES),
            0.0,
            "a confirmed true negative must keep false_negatives at 0"
        );

        // (c) Verification ON, a PRESENT key fed to the verify path (a synthetic
        // false negative — the oracle "said absent" for a key that IS present): the
        // authoritative scan contradicts it and the counter increments by 1 with
        // the SSTable's format.
        mc.reset();
        let got = rt
            .block_on(reader.verify_presence_oracle_negative(&table_id, &present_key))
            .expect("verify on contradiction");
        assert!(
            got,
            "a present key must be reported as a contradicted negative"
        );
        let m = mc.flush_and_collect();
        assert_eq!(
            m.counter_sum(catalog::READ_BLOOM_FALSE_NEGATIVES),
            1.0,
            "a contradicted negative must increment false_negatives by exactly 1"
        );
        assert_eq!(
            m.sum_where(
                catalog::READ_BLOOM_FALSE_NEGATIVES,
                &[(catalog::attr::SSTABLE_FORMAT, "big")],
            ),
            1.0,
            "the false negative must carry the offending SSTable's format"
        );
        presence_verification::set_enabled_for_testing(false);
        drop(rt);
    }

    // --- Requirement: Degraded read-path counter with bounded reason ---
    // The executor records honest fallbacks through `access_path::record`, the
    // public probe every fallback site funnels through. A fallback increments the
    // counter with its bounded reason label; a targeted path never does.
    {
        use cqlite_core::query::access_path::{self, AccessPath, FallbackReason};
        mc.reset();
        access_path::record(AccessPath::FallbackFullScan {
            reason: FallbackReason::NoSchema,
        });
        // A targeted path must NOT increment the degraded counter.
        access_path::record(AccessPath::PartitionLookup);
        let m = mc.flush_and_collect();
        assert_eq!(
            m.counter_sum(catalog::QUERY_DEGRADED_PATH),
            1.0,
            "exactly one degraded increment for the single fallback (targeted path adds none)"
        );
        assert_eq!(
            m.sum_where(
                catalog::QUERY_DEGRADED_PATH,
                &[(catalog::attr::FALLBACK_REASON, "no_schema")],
            ),
            1.0,
            "the degraded increment must carry the bounded fallback_reason label"
        );
    }
}

// ---------------------------------------------------------------------------
// Merge helpers (compaction over writer-produced SSTables).
// ---------------------------------------------------------------------------

mod merge {
    use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
    use cqlite_core::storage::write_engine::{STCSPolicy, WriteEngine, WriteEngineConfig};
    use std::collections::HashMap;

    /// A live write engine + its temp dir (kept alive) after flushing the inputs.
    pub struct Flows {
        pub engine: WriteEngine,
        _tmp: tempfile::TempDir,
    }

    fn pk_only_schema() -> TableSchema {
        TableSchema {
            keyspace: "obs2163".to_string(),
            table: "items".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![],
            columns: vec![col("id", "int", false), col("name", "text", true)],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn clustered_schema() -> TableSchema {
        TableSchema {
            keyspace: "obs2163".to_string(),
            table: "events".to_string(),
            partition_keys: vec![KeyColumn {
                name: "id".to_string(),
                data_type: "int".to_string(),
                position: 0,
            }],
            clustering_keys: vec![ClusteringColumn {
                name: "seq".to_string(),
                data_type: "int".to_string(),
                position: 0,
                order: Default::default(),
            }],
            columns: vec![
                col("id", "int", false),
                col("seq", "int", false),
                col("val", "text", true),
            ],
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    fn col(name: &str, ty: &str, nullable: bool) -> Column {
        Column {
            name: name.to_string(),
            data_type: ty.to_string(),
            nullable,
            default: None,
            is_static: false,
        }
    }

    fn open(dir: &std::path::Path, schema: TableSchema) -> WriteEngine {
        let cfg = WriteEngineConfig::new(dir.join("data"), dir.join("wal"), schema);
        let mut engine = WriteEngine::new(cfg).expect("engine");
        engine
            .set_merge_policy(Box::new(
                STCSPolicy::new(2, 32, 0.5, 1.5, 0).expect("policy"),
            ))
            .expect("set policy");
        engine
    }

    fn flush(engine: &mut WriteEngine) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(engine.flush())
            .expect("flush")
            .expect("sstable");
    }

    /// Two overlapping SSTables: id=1 in both (LWW-collapses), plus id=2 and id=3.
    pub fn run_overlapping_merge() -> Flows {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let mut engine = open(tmp.path(), pk_only_schema());
        engine
            .execute("INSERT INTO obs2163.items (id, name) VALUES (1, 'a')")
            .expect("w");
        engine
            .execute("INSERT INTO obs2163.items (id, name) VALUES (2, 'b')")
            .expect("w");
        flush(&mut engine);
        engine
            .execute("INSERT INTO obs2163.items (id, name) VALUES (1, 'a2')")
            .expect("w");
        engine
            .execute("INSERT INTO obs2163.items (id, name) VALUES (3, 'c')")
            .expect("w");
        flush(&mut engine);
        Flows { engine, _tmp: tmp }
    }

    /// A newer whole-row tombstone over an older live cell in the same slot.
    pub fn run_row_tombstone_merge() -> Flows {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let mut engine = open(tmp.path(), clustered_schema());
        engine
            .execute("INSERT INTO obs2163.events (id, seq, val) VALUES (1, 1, 'old')")
            .expect("w");
        flush(&mut engine);
        engine
            .execute("DELETE FROM obs2163.events WHERE id = 1 AND seq = 1")
            .expect("del");
        flush(&mut engine);
        Flows { engine, _tmp: tmp }
    }

    /// Drive maintenance compaction until a merge completes (or no work remains).
    pub fn compact(flows: &mut Flows) {
        // maintenance_step uses an internal block_on; call it OUTSIDE any runtime.
        let budget = std::time::Duration::from_secs(30);
        for _ in 0..10 {
            let report = flows
                .engine
                .maintenance_step(budget)
                .expect("maintenance_step");
            if !report.completed_merges.is_empty() {
                break;
            }
            if !report.pending_compaction {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Reader fixtures (real BIG fixture + a writer-produced schema-attached reader).
// ---------------------------------------------------------------------------

mod fixtures {
    use cqlite_core::platform::Platform;
    use cqlite_core::schema::registry::{SchemaRegistry, SchemaRegistryConfig, SchemaSource};
    use cqlite_core::schema::{Column, KeyColumn, TableSchema};
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
    use cqlite_core::types::{RowKey, TableId};
    use cqlite_core::Config;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    fn datasets_root() -> PathBuf {
        match std::env::var("CQLITE_DATASETS_ROOT") {
            Ok(root) => PathBuf::from(root),
            Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../test-data/datasets"),
        }
    }

    /// Open a real BIG (nb) fixture SSTable reader (has a Filter.db bloom filter).
    pub fn open_big_reader() -> SSTableReader {
        let parent = datasets_root().join("sstables").join("test_basic");
        let table_dir = std::fs::read_dir(&parent)
            .unwrap_or_else(|e| panic!("read {}: {e} — fetch datasets first", parent.display()))
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with("simple_table-"))
            .map(|e| e.path())
            .expect("simple_table fixture present");
        let data_file = std::fs::read_dir(&table_dir)
            .expect("read table dir")
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().ends_with("-Data.db"))
            .expect("Data.db present");

        let rt = tokio::runtime::Runtime::new().expect("rt");
        let config = Config::default();
        let platform = Arc::new(rt.block_on(Platform::new(&config)).expect("platform"));
        rt.block_on(SSTableReader::open(&data_file, &config, platform))
            .expect("open reader")
    }

    fn writer_schema() -> TableSchema {
        TableSchema {
            keyspace: "obsfn".to_string(),
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
            comments: HashMap::new(),
            dropped_columns: HashMap::new(),
        }
    }

    /// Write one row, flush, and open a schema-attached reader over the produced
    /// Data.db. Returns the reader, a matching TableId, and the present key bytes.
    /// The reader can authoritatively scan for the key (schema attached), which the
    /// false-negative verification needs.
    pub fn open_writer_reader_with_present_key(
    ) -> (SSTableReader, TableId, [u8; 4], tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let schema = writer_schema();
        let cfg = WriteEngineConfig::new(
            tmp.path().join("data"),
            tmp.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(cfg).expect("engine");
        engine
            .execute("INSERT INTO obsfn.rows (id, name) VALUES (7, 'seven')")
            .expect("write");

        let rt = tokio::runtime::Runtime::new().expect("rt");
        let info = rt
            .block_on(engine.flush())
            .expect("flush")
            .expect("sstable");
        let data_file = info.data_path.clone();

        let config = Config::default();
        let platform = Arc::new(rt.block_on(Platform::new(&config)).expect("platform"));
        let mut reader = rt
            .block_on(SSTableReader::open(&data_file, &config, platform.clone()))
            .expect("open reader");

        // Attach the schema so the authoritative scan can decode the row.
        let registry = rt
            .block_on(SchemaRegistry::new(
                SchemaRegistryConfig::default(),
                platform.clone(),
                config.clone(),
            ))
            .expect("build registry");
        rt.block_on(registry.register_schema(schema, SchemaSource::Manual))
            .expect("register schema");
        let registry = Arc::new(tokio::sync::RwLock::new(registry));
        rt.block_on(reader.attach_schema_registry(registry));

        // Confirm the reader really can find the present key (so the verify scan
        // is meaningful), then hand back the pieces the test needs.
        let table_id = TableId::from("rows");
        let present_key = 7i32.to_be_bytes();
        let found = rt
            .block_on(reader.get(&table_id, &RowKey::from(present_key.to_vec())))
            .expect("get present key");
        assert!(
            found.is_some(),
            "writer fixture reader must be able to read back the present key for the \
             false-negative scan to be meaningful"
        );
        drop(rt);
        (reader, table_id, present_key, tmp)
    }
}
