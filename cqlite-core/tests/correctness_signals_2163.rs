//! Correctness / silent-miss observability signals (issue #2163).
//!
//! Scenario coverage for the `correctness-signals` OpenSpec change. Every metric
//! assertion drives a REAL read / merge / query through a public surface (a
//! compaction via `WriteEngine::maintenance_step`, a point read via
//! `SSTableReader`, a `SELECT` via `Database`) and asserts against the emitted
//! catalog metrics captured by the shared in-memory OTLP exporter.
//!
//! The presence-oracle opt-in VERIFICATION SWITCH scenarios (the false-negative
//! confirmation scan, its `get_with_resolution`/`ObservabilityConfig` wiring, the
//! no-double-scan invariant, and the degraded read-path counter) live in the
//! sibling file `correctness_signals_2163_verify.rs` — split out (campsite rule,
//! epic #1135) once this file's own additions crossed the ~1500-line test-file
//! threshold; see that file's header for why a separate test BINARY is safe here
//! (each `tests/*.rs` file is its own process with independent global state).
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
use serial_test::serial;

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

// Roborev r6 (LOW, #2163): this test mutates PROCESS-GLOBAL state (the
// `presence_verification` switch, `CQLITE_VERIFY_PRESENCE_ORACLE` /
// `std::env::set_var`/`remove_var`, and the shared observability metrics
// singleton) without any synchronization against another `#[test]` in this
// same binary. There is currently only ONE test function here, so this is
// defensive/future-proofing rather than a live bug — but `#[serial]` costs
// nothing and matches the established repo idiom (e.g.
// `issue_1575_candidate_key_hash_hoist.rs`) for any test that touches process
// env or global switches, protecting against a future second test added to
// this file (or a test-harness change) racing on the same global state.
#[test]
#[serial]
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
    //
    // This is ALSO the roborev r5 (#2163) regression pin: `run_row_tombstone_merge`
    // uses a CLUSTERED schema (`obs2163.events`, clustering key `seq`), so the
    // compaction read-back carries a `seq` clustering-key PSEUDO-CELL alongside
    // the real `val` data cell (mirrors `extract_clustering_key`'s read-back
    // contract, pinned by the existing unit test
    // `issue_921_clustered_row_with_only_purgeable_cell_tombstone_emits_nothing`).
    // The row tombstone shadows BOTH cells (both were written before the
    // delete), but `seq` is not real data — only `val` may count. Asserting the
    // EXACT count (not merely `>= 1.0`) catches the inflation: without the fix
    // this scenario reports 2 suppressed (val + seq); confirmed empirically
    // while developing this fix by disabling the exclusion.
    {
        let mut flows = merge::run_row_tombstone_merge();
        mc.reset();
        merge::compact(&mut flows);
        let m = mc.flush_and_collect();
        let suppressed = m.counter_sum(catalog::COMPACTION_TOMBSTONES_SUPPRESSED);
        let emitted = m.counter_sum(catalog::COMPACTION_TOMBSTONES_EMITTED);
        assert_eq!(
            suppressed,
            1.0,
            "cqlite.compaction.tombstones_suppressed must count DATA cells only — exactly 1 \
             (the shadowed `val` cell), excluding the `seq` clustering-key pseudo-cell that the \
             read-back path retains for round-tripping but is not real data; entry: {:?}",
            m.find(catalog::COMPACTION_TOMBSTONES_SUPPRESSED)
        );
        assert!(
            emitted >= 1.0,
            "the retained row-tombstone marker must count >=1 emitted; entry: {:?}",
            m.find(catalog::COMPACTION_TOMBSTONES_EMITTED)
        );
    }

    // --- Roborev blocker #2 (#2163): retained tombstone COEXISTING with a
    // strictly-newer live cell must ALSO count as emitted (the `build()` branch
    // distinct from the sole-tombstone-output case above; underreported before
    // the fix — that branch never incremented `emitted`). ---
    {
        let mut flows = merge::run_row_tombstone_with_newer_cell_merge();
        mc.reset();
        merge::compact(&mut flows);
        let m = mc.flush_and_collect();
        assert!(
            m.counter_sum(catalog::COMPACTION_TOMBSTONES_EMITTED) >= 1.0,
            "a row tombstone retained alongside a newer surviving cell must count >=1 emitted; \
             entry: {:?}",
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

    // --- SSTable-pruned counter, BTI (`da`) branch (roborev blocker #1, #2163) ---
    // The BTI candidate-prune (`SSTableManager::prune_candidates`) routes through
    // the SAME public `might_contain_partition_encoded` the fix wires the counter
    // into — exercised here directly (the "lowest public level" the BTI trie-miss
    // prune reaches) against a real `test_da` BTI fixture, proving the
    // `format="bti"` scenario arm the spec requires.
    {
        use cqlite_core::storage::sstable::bti::encode_partition_key_for_bti_trie;

        let reader = fixtures::open_bti_reader();
        mc.reset();
        let mut expected_pruned = 0u64;
        for i in 0..64u32 {
            // 16-byte pseudo-UUIDs that do not exist in the fixture (whose real
            // partition keys are UUIDs from a fixed, disjoint byte pattern).
            let key = [
                0xDEu8,
                0xAD,
                0xBE,
                0xEF,
                0xFE,
                0xED,
                0xFA,
                0xCE,
                (i >> 8) as u8,
                i as u8,
                0xAB,
                0xCD,
                0x00,
                0x01,
                0x02,
                0x03,
            ];
            let encoded = encode_partition_key_for_bti_trie(&key);
            if !reader.might_contain_partition_encoded(&key, &encoded) {
                expected_pruned += 1;
            }
        }
        let m = mc.flush_and_collect();
        assert!(
            expected_pruned >= 1,
            "with 64 absent pseudo-UUID probes the BTI trie must report >=1 definitive miss"
        );
        assert_eq!(
            m.counter_sum(catalog::READ_SSTABLES_PRUNED),
            expected_pruned as f64,
            "cqlite.read.sstables_pruned must increment once per definitively-absent BTI probe \
             (the roborev-flagged gap: the candidate-prune path must emit for BTI too)"
        );
        assert_eq!(
            m.sum_where(
                catalog::READ_SSTABLES_PRUNED,
                &[(catalog::attr::SSTABLE_FORMAT, "bti")],
            ),
            expected_pruned as f64,
            "every BTI prune must carry cqlite.sstable.format=bti"
        );
    }

    // --- Roborev r4 blocker #1 (#2163): the PRIMARY point-read path must emit
    // `sstables_pruned` too — not only the `might_contain_partition[_encoded]`
    // candidate-prune helpers. This IS the spec's own scenario: "a partition
    // point lookup ... through the public read surface" over a table with
    // multiple SSTables, present in one generation and absent from another. ---
    {
        use cqlite_core::types::RowKey;

        let (reader_present, reader_absent, table_id, _tmp) =
            fixtures::open_two_generation_readers();
        let rt = tokio::runtime::Runtime::new().expect("rt");

        // Search among gen 1's own ids for one gen 2's bloom filter ALSO reports
        // absent (bloom filters can rarely false-positive on an unrelated key;
        // searching — not assuming — keeps this deterministic, same technique as
        // the BTI in-range search above).
        let mut probe_id: Option<i32> = None;
        for candidate in 0..20i32 {
            let key_bytes = candidate.to_be_bytes();
            if !reader_absent.might_contain_partition(&key_bytes) {
                probe_id = Some(candidate);
                break;
            }
        }
        let probe_id = probe_id.expect(
            "at least one of gen 1's 20 ids must show as absent in gen 2's bloom filter \
             (expected false-positive rate ~1%)",
        );
        let probe_key = probe_id.to_be_bytes();

        mc.reset();
        let found_in_gen1 = rt
            .block_on(reader_present.get_with_resolution(
                &table_id,
                &RowKey::from(probe_key.to_vec()),
                false,
            ))
            .expect("get on the generation holding the key");
        let found_in_gen2 = rt
            .block_on(reader_absent.get_with_resolution(
                &table_id,
                &RowKey::from(probe_key.to_vec()),
                false,
            ))
            .expect("get on the generation NOT holding the key");
        drop(rt);

        assert!(
            found_in_gen1.is_some(),
            "the generation that actually holds id={probe_id} must return it"
        );
        assert!(
            found_in_gen2.is_none(),
            "the OTHER generation must report id={probe_id} absent"
        );

        let m = mc.flush_and_collect();
        assert_eq!(
            m.counter_sum(catalog::READ_SSTABLES_PRUNED),
            1.0,
            "the PRIMARY public get_with_resolution() point-read path must emit \
             cqlite.read.sstables_pruned exactly once for the excluded generation \
             (the gen-1 hit must emit nothing); entry: {:?}",
            m.find(catalog::READ_SSTABLES_PRUNED)
        );
        assert_eq!(
            m.sum_where(
                catalog::READ_SSTABLES_PRUNED,
                &[(catalog::attr::SSTABLE_FORMAT, "big")],
            ),
            1.0,
            "the prune must carry cqlite.sstable.format=big"
        );
    }

    // --- Roborev r6 (#2163): the CORE silent-miss case — a candidate
    // eliminated by `SSTableManager::prune_candidates` (the multi-generation
    // path) must ALSO be opt-in-verified, not just a single reader's own
    // `get_with_resolution`. A fault-injected oracle (a Filter.db whose bit
    // array is zeroed post-write — a real, on-disk corruption, not a mock)
    // wrongly excludes gen 2 even though it genuinely holds the probed key.
    // Switch ON must catch the contradiction; switch OFF must cost nothing. ---
    {
        use cqlite_core::platform::Platform;
        use cqlite_core::schema::registry::{SchemaRegistry, SchemaRegistryConfig, SchemaSource};
        use cqlite_core::storage::sstable::SSTableManager;
        use cqlite_core::Config;
        use std::sync::Arc;

        let (data_dir, filter_path, table_id, _tmp) = fixtures::write_two_generations_for_manager();

        // Fault-inject: zero the bloom filter's BIT ARRAY (bytes after the
        // 8-byte header: `hash_count: i32 BE`, `num_longs: i32 BE`) so the
        // filter parses as STRUCTURALLY VALID but reports every key absent —
        // a real (simulated) Filter.db corruption, matching `bloom.rs`'s own
        // documented failure mode ("would fail OPEN (always report 'not
        // present'), producing false negatives").
        let mut filter_bytes = std::fs::read(&filter_path).expect("read gen2 Filter.db");
        assert!(
            filter_bytes.len() > 8,
            "gen2 Filter.db must have a non-empty bit array to corrupt"
        );
        for b in &mut filter_bytes[8..] {
            *b = 0;
        }
        std::fs::write(&filter_path, &filter_bytes).expect("write corrupted Filter.db");

        let rt = tokio::runtime::Runtime::new().expect("rt");
        let config = Config::default();
        let platform = Arc::new(rt.block_on(Platform::new(&config)).expect("platform"));

        // Attach the schema via a registry so the manager's readers can decode
        // rows (mirrors the other fixtures' schema-attach step).
        let registry = rt
            .block_on(SchemaRegistry::new(
                SchemaRegistryConfig::default(),
                platform.clone(),
                config.clone(),
            ))
            .expect("build registry");
        rt.block_on(registry.register_schema(fixtures::writer_schema(), SchemaSource::Manual))
            .expect("register schema");
        let registry = Arc::new(tokio::sync::RwLock::new(registry));

        let manager = rt
            .block_on(SSTableManager::new(
                &data_dir,
                &config,
                platform,
                Some(registry),
            ))
            .expect("open manager over both generations (one with a corrupted Filter.db)");

        // A key from gen 2's own range (2000..2010) — genuinely present in
        // gen2's Data.db, but the corrupted Filter.db now reports it absent,
        // so `prune_candidates` wrongly excludes gen2 from `candidates`.
        let probe_key = 2005i32.to_be_bytes();

        // (a) Verification OFF (default): the read still (silently) misses the
        // row — that IS the silent-miss bug this issue targets — but the
        // opt-in scan must not run at all (zero extra cost).
        presence_verification::set_enabled_for_testing(false);
        mc.reset();
        let (rows_off, _engaged) = rt
            .block_on(manager.scan_partition_with_cell_metadata(
                &table_id,
                &probe_key,
                Some(&fixtures::writer_schema()),
            ))
            .expect("scan with verification off");
        let m = mc.flush_and_collect();
        assert!(
            rows_off.is_empty(),
            "the corrupted Filter.db must silently exclude gen2 (the bug this switch detects) \
             when verification is off"
        );
        assert_eq!(
            m.counter_sum(catalog::READ_BLOOM_FALSE_NEGATIVES),
            0.0,
            "verification OFF must never emit false_negatives (zero extra scan cost)"
        );

        // (b) Verification ON, the SAME key: the opt-in authoritative scan of
        // the excluded gen2 candidate finds the key, contradicting the
        // (corrupted) oracle's negative — `cqlite.read.bloom.false_negatives`
        // must increment by EXACTLY 1 with `format=big`.
        presence_verification::set_enabled_for_testing(true);
        mc.reset();
        let (_rows_on, _engaged) = rt
            .block_on(manager.scan_partition_with_cell_metadata(
                &table_id,
                &probe_key,
                Some(&fixtures::writer_schema()),
            ))
            .expect("scan with verification on");
        let m = mc.flush_and_collect();
        assert_eq!(
            m.counter_sum(catalog::READ_BLOOM_FALSE_NEGATIVES),
            1.0,
            "the multi-generation candidate-prune false negative must be caught EXACTLY once \
             when verification is on; entry: {:?}",
            m.find(catalog::READ_BLOOM_FALSE_NEGATIVES)
        );
        assert_eq!(
            m.sum_where(
                catalog::READ_BLOOM_FALSE_NEGATIVES,
                &[(catalog::attr::SSTABLE_FORMAT, "big")],
            ),
            1.0,
            "the contradicted negative must carry cqlite.sstable.format=big"
        );

        presence_verification::set_enabled_for_testing(false);
        drop(rt);
    }

    // --- Roborev r7 (#2163): the REVERSE-SCAN FAST PATH (`candidates.len() ==
    // 1` branch of `scan_partition_clustering_reverse`) serves the read
    // DIRECTLY from the single admitted candidate and never falls through to
    // the reconciling fallback — so a false negative wrongly excluding a
    // co-holding generation on THIS branch was never verified (unlike the r6
    // multi-generation-manager case, which the `!= 1` fallback DOES cover via
    // `scan_partition_clustering`'s own `verify_pruned_candidates` call). Drives
    // the PUBLIC `ORDER BY ... DESC` reverse read path end-to-end through
    // `Database::execute`, not the manager directly. ---
    {
        use cqlite_core::ingestion::{ingest, IngestionConfig};

        let (data_dir, filter_path, _tmp) = fixtures::write_wide_partition_two_generations();

        // Fault-inject: zero gen B's Filter.db bit array (same technique as the
        // r6 manager test) — structurally valid, but reports every key absent,
        // so `prune_candidates` wrongly excludes gen B and
        // `scan_partition_clustering_reverse` takes the `len() == 1` fast path
        // serving ONLY gen A's 100 rows.
        let mut filter_bytes = std::fs::read(&filter_path).expect("read gen B Filter.db");
        assert!(
            filter_bytes.len() > 8,
            "gen B Filter.db must have a non-empty bit array to corrupt"
        );
        for b in &mut filter_bytes[8..] {
            *b = 0;
        }
        std::fs::write(&filter_path, &filter_bytes).expect("write corrupted Filter.db");

        // A temp CQL schema file so `ingest()` can register query-time decode
        // metadata for the table (independent of the WriteEngine's own schema).
        let schema_dir = tempfile::TempDir::new().expect("schema tmp");
        let schema_path = schema_dir.path().join("wide.cql");
        std::fs::write(
            &schema_path,
            "CREATE TABLE obsfn.wide (id int, seq int, val text, PRIMARY KEY (id, seq));",
        )
        .expect("write schema file");

        let rt = tokio::runtime::Runtime::new().expect("rt");
        let db = rt
            .block_on(ingest(IngestionConfig {
                schema_paths: vec![schema_path],
                data_dir,
                version_hint: Some("5.0".to_string()),
                core_config: cqlite_core::Config::default(),
                table_directory_filter: None,
            }))
            .expect("ingest over both generations (gen B has a corrupted Filter.db)")
            .database;

        // (a) Verification OFF (default): the reverse fast path silently serves
        // ONLY gen A's 100 rows (the silent-miss bug this switch detects; the
        // read wrongly took the single-generation direct-serve path instead of
        // reconciling with gen B's 5 extra rows) — and no confirmation scan runs.
        presence_verification::set_enabled_for_testing(false);
        mc.reset();
        let res_off = rt
            .block_on(db.execute("SELECT seq FROM obsfn.wide WHERE id = 1 ORDER BY seq DESC"))
            .expect("reverse query, verification off");
        let m = mc.flush_and_collect();
        assert_eq!(
            res_off.rows.len(),
            100,
            "the reverse fast path must silently miss gen B's 5 extra rows when verification is \
             off — sees only gen A's 100 rows"
        );
        assert_eq!(
            m.counter_sum(catalog::READ_BLOOM_FALSE_NEGATIVES),
            0.0,
            "verification OFF must never emit false_negatives (zero extra scan cost)"
        );

        // (b) Verification ON, the SAME (still-corrupted) data: the reverse
        // fast path's own pruned-candidate verification (roborev r7 fix) must
        // catch the contradiction exactly once.
        presence_verification::set_enabled_for_testing(true);
        mc.reset();
        let _res_on = rt
            .block_on(db.execute("SELECT seq FROM obsfn.wide WHERE id = 1 ORDER BY seq DESC"))
            .expect("reverse query, verification on");
        let m = mc.flush_and_collect();
        assert_eq!(
            m.counter_sum(catalog::READ_BLOOM_FALSE_NEGATIVES),
            1.0,
            "the reverse-scan fast path's wrongly-pruned gen B must be caught EXACTLY once when \
             verification is on; entry: {:?}",
            m.find(catalog::READ_BLOOM_FALSE_NEGATIVES)
        );
        assert_eq!(
            m.sum_where(
                catalog::READ_BLOOM_FALSE_NEGATIVES,
                &[(catalog::attr::SSTABLE_FORMAT, "big")],
            ),
            1.0,
            "the contradicted negative must carry cqlite.sstable.format=big"
        );

        presence_verification::set_enabled_for_testing(false);
        drop(rt);
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
                col("extra", "text", true),
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

    /// Roborev blocker #2 (#2163): a row tombstone RETAINED alongside a STRICTLY
    /// NEWER live cell (the common "coexisting" case — `build()`'s
    /// `!surviving.is_empty()` branch — distinct from `run_row_tombstone_merge`'s
    /// sole-tombstone-output branch). Explicit `USING TIMESTAMP` pins the ordering
    /// deterministically (no wall-clock race): INSERT `val` at t=1000, whole-row
    /// DELETE at t=2000 (shadows `val`), then INSERT `extra` at t=3000 (survives the
    /// deletion) — the row tombstone must still be carried into the merged output
    /// via `with_row_deletion` alongside the surviving `extra` cell.
    pub fn run_row_tombstone_with_newer_cell_merge() -> Flows {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let mut engine = open(tmp.path(), clustered_schema());
        engine
            .execute(
                "INSERT INTO obs2163.events (id, seq, val) VALUES (2, 1, 'old') \
                 USING TIMESTAMP 1000",
            )
            .expect("w1");
        flush(&mut engine);
        engine
            .execute("DELETE FROM obs2163.events USING TIMESTAMP 2000 WHERE id = 2 AND seq = 1")
            .expect("del");
        flush(&mut engine);
        engine
            .execute(
                "INSERT INTO obs2163.events (id, seq, extra) VALUES (2, 1, 'new') \
                 USING TIMESTAMP 3000",
            )
            .expect("w2");
        flush(&mut engine);
        Flows { engine, _tmp: tmp }
    }

    /// Drive maintenance compaction to full convergence (every eligible merge
    /// round completes, not just the first). A bucketed STCS policy may need MORE
    /// THAN ONE `maintenance_step` round to combine 3+ generations down to one
    /// SSTable (e.g. round 1 merges gens 1+2, round 2 merges that output with
    /// gen 3) — breaking after the FIRST completed merge (the pre-#2163-fix-round
    /// shape) would silently leave later generations unmerged. Only stop once a
    /// round does NO work at all (nothing completed AND nothing pending): that is
    /// the sole idle signal `maintenance_step` gives (`pending_compaction` reflects
    /// an in-progress merge continuing past its budget, not "more candidates
    /// exist" — see `maintenance.rs`), so an idle round with tiny test data means
    /// every candidate has been folded in.
    pub fn compact(flows: &mut Flows) {
        // maintenance_step uses an internal block_on; call it OUTSIDE any runtime.
        let budget = std::time::Duration::from_secs(30);
        for _ in 0..10 {
            let report = flows
                .engine
                .maintenance_step(budget)
                .expect("maintenance_step");
            if report.completed_merges.is_empty() && !report.pending_compaction {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Reader fixtures (real BIG/BTI corpus fixtures + multi-generation
// writer-produced SSTables for the candidate-prune scenarios).
// ---------------------------------------------------------------------------

mod fixtures {
    use cqlite_core::platform::Platform;
    use cqlite_core::schema::registry::{SchemaRegistry, SchemaRegistryConfig, SchemaSource};
    use cqlite_core::schema::{ClusteringColumn, Column, KeyColumn, TableSchema};
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};
    use cqlite_core::types::TableId;
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

    /// Open a real BTI (`da`) fixture SSTable reader (has a `Partitions.db` trie —
    /// the authoritative BTI presence oracle). Corpus: `test_da/simple_table-*`.
    pub fn open_bti_reader() -> SSTableReader {
        let parent = datasets_root().join("sstables").join("test_da");
        let table_dir = std::fs::read_dir(&parent)
            .unwrap_or_else(|e| panic!("read {}: {e} — fetch datasets first", parent.display()))
            .filter_map(|e| e.ok())
            .find(|e| e.file_name().to_string_lossy().starts_with("simple_table-"))
            .map(|e| e.path())
            .expect("test_da/simple_table BTI fixture present");
        let data_file = std::fs::read_dir(&table_dir)
            .expect("read table dir")
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().ends_with("-Data.db"))
            .expect("Data.db present");

        let rt = tokio::runtime::Runtime::new().expect("rt");
        let config = Config::default();
        let platform = Arc::new(rt.block_on(Platform::new(&config)).expect("platform"));
        let reader = rt
            .block_on(SSTableReader::open(&data_file, &config, platform))
            .expect("open BTI reader");
        assert!(
            reader.is_bti(),
            "test_da/simple_table-* must open as a BTI reader (Partitions.db present)"
        );
        reader
    }

    pub fn writer_schema() -> TableSchema {
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

    /// Two DISJOINT-key SSTable generations of the SAME table (no compaction),
    /// opened as independent `SSTableReader`s — the multi-SSTable point-lookup
    /// scenario the spec names directly (roborev r4, #2163). Generation 1 holds
    /// ids `0..20`; generation 2 holds ids `1000..1020` (a disjoint range so a
    /// probe key from gen 1 is genuinely absent from gen 2's Data.db, not merely
    /// absent from the writer's `INSERT` set).
    pub fn open_two_generation_readers(
    ) -> (SSTableReader, SSTableReader, TableId, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let schema = writer_schema();
        let cfg = WriteEngineConfig::new(
            tmp.path().join("data"),
            tmp.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(cfg).expect("engine");
        let rt = tokio::runtime::Runtime::new().expect("rt");

        for id in 0..20i32 {
            engine
                .execute(&format!(
                    "INSERT INTO obsfn.rows (id, name) VALUES ({id}, 'g1-{id}')"
                ))
                .expect("write gen1");
        }
        let info1 = rt
            .block_on(engine.flush())
            .expect("flush gen1")
            .expect("sstable gen1");

        for id in 1000..1020i32 {
            engine
                .execute(&format!(
                    "INSERT INTO obsfn.rows (id, name) VALUES ({id}, 'g2-{id}')"
                ))
                .expect("write gen2");
        }
        let info2 = rt
            .block_on(engine.flush())
            .expect("flush gen2")
            .expect("sstable gen2");

        let config = Config::default();
        let platform = Arc::new(rt.block_on(Platform::new(&config)).expect("platform"));
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

        let mut reader1 = rt
            .block_on(SSTableReader::open(
                &info1.data_path,
                &config,
                platform.clone(),
            ))
            .expect("open gen1 reader");
        rt.block_on(reader1.attach_schema_registry(registry.clone()));

        let mut reader2 = rt
            .block_on(SSTableReader::open(&info2.data_path, &config, platform))
            .expect("open gen2 reader");
        rt.block_on(reader2.attach_schema_registry(registry));

        drop(rt);
        (reader1, reader2, TableId::from("rows"), tmp)
    }

    /// Two DISJOINT-key SSTable generations under ONE table directory, written
    /// but with NO readers opened yet — so the caller can corrupt one
    /// generation's Filter.db BEFORE constructing an `SSTableManager` over the
    /// shared `data` directory (roborev r6, #2163's multi-generation
    /// candidate-prune false-negative test). Gen 1 holds ids `0..10`; gen 2
    /// holds ids `2000..2010` (disjoint). Returns the `data` root (pass to
    /// `SSTableManager::new`), gen 2's `Filter.db` path, the fully-qualified
    /// `TableId`, and the `TempDir` (kept alive).
    pub fn write_two_generations_for_manager() -> (PathBuf, PathBuf, TableId, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let schema = writer_schema();
        let data_dir = tmp.path().join("data");
        let cfg = WriteEngineConfig::new(data_dir.clone(), tmp.path().join("wal"), schema);
        let mut engine = WriteEngine::new(cfg).expect("engine");
        let rt = tokio::runtime::Runtime::new().expect("rt");

        for id in 0..10i32 {
            engine
                .execute(&format!(
                    "INSERT INTO obsfn.rows (id, name) VALUES ({id}, 'g1-{id}')"
                ))
                .expect("write gen1");
        }
        rt.block_on(engine.flush())
            .expect("flush gen1")
            .expect("sstable gen1");

        for id in 2000..2010i32 {
            engine
                .execute(&format!(
                    "INSERT INTO obsfn.rows (id, name) VALUES ({id}, 'g2-{id}')"
                ))
                .expect("write gen2");
        }
        let info2 = rt
            .block_on(engine.flush())
            .expect("flush gen2")
            .expect("sstable gen2");
        drop(rt);

        let filter_path = info2
            .filter_path
            .clone()
            .expect("gen2 must have a Filter.db (default bloom_filter_fp_chance)");

        (data_dir, filter_path, TableId::from("obsfn.rows"), tmp)
    }

    fn wide_schema() -> TableSchema {
        TableSchema {
            keyspace: "obsfn".to_string(),
            table: "wide".to_string(),
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
                Column {
                    name: "id".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "seq".to_string(),
                    data_type: "int".to_string(),
                    nullable: false,
                    default: None,
                    is_static: false,
                },
                Column {
                    name: "val".to_string(),
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

    /// A WIDE partition (`id = 1`) split across TWO generations, for the
    /// reverse-scan fast-path candidate-prune test (roborev r7, #2163). Gen A
    /// holds 100 clustering rows carrying a 1 KiB `val` each (~100 KiB total —
    /// comfortably crossing the 64 KiB promoted-index threshold, so
    /// `big_reverse_partition_rows` actually engages: a promoted index is only
    /// emitted once a partition crosses that boundary). Gen B holds 5 MORE
    /// clustering rows for the SAME partition (small; it doesn't itself need to
    /// be wide) — UNCORRUPTED, both generations legitimately hold `id = 1`, so
    /// `candidates.len() == 2` and the reverse fast path correctly declines in
    /// favor of the reconciling in-memory-sort fallback. Returns the `data` root
    /// (pass to `ingest()`), gen B's `Filter.db` path (for corruption), and the
    /// `TempDir` (kept alive).
    pub fn write_wide_partition_two_generations() -> (PathBuf, PathBuf, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let schema = wide_schema();
        let data_dir = tmp.path().join("data");
        let cfg = WriteEngineConfig::new(data_dir.clone(), tmp.path().join("wal"), schema);
        let mut engine = WriteEngine::new(cfg).expect("engine");
        let rt = tokio::runtime::Runtime::new().expect("rt");

        let blob = "x".repeat(1024);
        for seq in 0..100i32 {
            engine
                .execute(&format!(
                    "INSERT INTO obsfn.wide (id, seq, val) VALUES (1, {seq}, '{blob}')"
                ))
                .expect("write gen A row");
        }
        rt.block_on(engine.flush())
            .expect("flush gen A")
            .expect("sstable gen A");

        for seq in 100..105i32 {
            engine
                .execute(&format!(
                    "INSERT INTO obsfn.wide (id, seq, val) VALUES (1, {seq}, 'genB-{seq}')"
                ))
                .expect("write gen B row");
        }
        let info_b = rt
            .block_on(engine.flush())
            .expect("flush gen B")
            .expect("sstable gen B");
        drop(rt);

        let filter_path = info_b
            .filter_path
            .clone()
            .expect("gen B must have a Filter.db (default bloom_filter_fp_chance)");

        (data_dir, filter_path, tmp)
    }
}
