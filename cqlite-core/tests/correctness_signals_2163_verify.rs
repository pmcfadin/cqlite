//! Correctness / silent-miss observability signals (issue #2163) — presence-
//! oracle verification switch coverage.
//!
//! Split out of `correctness_signals_2163.rs` (campsite rule, epic #1135): that
//! file's own additions pushed it past the ~1500-line test-file threshold, and
//! everything here is thematically self-contained (the opt-in false-negative
//! verification switch, its wiring through the public `get_with_resolution` read
//! path, its `ObservabilityConfig` plumbing, and the degraded read-path counter)
//! and does not depend on the merge/tombstone fixtures the sibling file keeps.
//!
//! # Why one serial metric test
//!
//! The production metric helpers record through a single process-global `Meter`
//! bound on first use, so the in-memory meter provider is process-wide and uses
//! DELTA temporality — the same discipline `observability_correctness.rs` and
//! `correctness_signals_2163.rs` document. Splitting into a SEPARATE test binary
//! (this file) is safe: each `tests/*.rs` file compiles to its own process with
//! its own independent meter/switch/env state, so there is no cross-file race —
//! only intra-file races would matter, and this file (like its sibling) keeps
//! every scenario in ONE `#[serial]` test function.
//!
//! Run with:
//! ```text
//! cargo test -p cqlite-core \
//!   --features observability-testing,cli-helpers,write-support \
//!   --test correctness_signals_2163_verify
//! ```

#![cfg(all(feature = "observability-testing", feature = "write-support"))]

use cqlite_core::observability::{catalog, testing};
use cqlite_core::storage::sstable::reader::presence_verification;
use serial_test::serial;

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
fn correctness_signals_verify_switch() {
    let mc = testing::metrics_capture();

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

    // --- Roborev fold-in #3 (#2163): `get()`-level false-negative wiring ---
    // Proves the verification switch is wired through the PUBLIC
    // `get_with_resolution` read-path entry point end-to-end — not just callable
    // on the `verify_presence_oracle_negative` method directly. Uses the
    // process-global `scan_for_key_call_count()` (issue #831 pattern) as the
    // observable proxy for "the authoritative confirmation scan ran": OFF must
    // leave the count unchanged for an absent key; ON must increment it for the
    // SAME key.
    {
        use cqlite_core::storage::sstable::reader::SSTableReader;
        use cqlite_core::types::RowKey;

        let (reader, table_id, _tmp) = fixtures::open_writer_reader_with_many_rows(50);
        let rt = tokio::runtime::Runtime::new().expect("rt");

        // Search for an absent probe key whose token falls WITHIN this reader's
        // authoritative Summary bound (else the C5 range short-circuit in
        // `get_with_resolution` returns absence before ever reaching the
        // presence-oracle / verify hook — a token-order fact independent of int
        // order, so we search rather than assume). Verification is ON during the
        // search. Accept ONLY a candidate whose scan-count delta is EXACTLY 1: that
        // is the clean signature of "my verify hook ran its one confirmation scan
        // and nothing else did" — a candidate that also trips the natural BIG
        // resolution's OWN scan_for_key fallback (a rare bloom false-positive) would
        // show delta 2 and is deliberately skipped, since re-probing it with
        // verification OFF would then ALSO show a nonzero delta from that unrelated
        // natural-fallback path, not from the switch.
        presence_verification::set_enabled_for_testing(true);
        let mut in_range_absent_key: Option<[u8; 4]> = None;
        for candidate in 1_000_000i32..1_000_064 {
            let key_bytes = candidate.to_be_bytes();
            let before = SSTableReader::scan_for_key_call_count();
            let res = rt
                .block_on(reader.get_with_resolution(
                    &table_id,
                    &RowKey::from(key_bytes.to_vec()),
                    false,
                ))
                .expect("get on candidate probe");
            let after = SSTableReader::scan_for_key_call_count();
            assert!(
                res.is_none(),
                "candidate {candidate} must be genuinely absent"
            );
            if after == before + 1 {
                in_range_absent_key = Some(key_bytes);
                break;
            }
        }
        let absent_key = in_range_absent_key.expect(
            "at least one of 64 candidates must land within the Summary bound (expected token \
             coverage from 50 samples is ~96%) — proves the ON case reaches get_with_resolution's \
             verify hook rather than being short-circuited by C5",
        );

        // (a) OFF: re-probing the SAME now-confirmed-in-range absent key must NOT
        // run a confirmation scan.
        presence_verification::set_enabled_for_testing(false);
        let before = SSTableReader::scan_for_key_call_count();
        let res = rt
            .block_on(reader.get_with_resolution(
                &table_id,
                &RowKey::from(absent_key.to_vec()),
                false,
            ))
            .expect("get with verification off");
        let after = SSTableReader::scan_for_key_call_count();
        assert!(res.is_none(), "the probe key must remain reported absent");
        assert_eq!(
            after, before,
            "verification OFF must not run a confirmation scan through get_with_resolution"
        );

        // (b) ON: the SAME key, through the SAME public entry point, must run the
        // confirmation scan (delta >= 1) — end-to-end wiring evidence — while the
        // returned value is unchanged (a side-channel check only) and the
        // false-negative counter stays 0 (a true negative).
        presence_verification::set_enabled_for_testing(true);
        mc.reset();
        let before = SSTableReader::scan_for_key_call_count();
        let res = rt
            .block_on(reader.get_with_resolution(
                &table_id,
                &RowKey::from(absent_key.to_vec()),
                false,
            ))
            .expect("get with verification on");
        let after = SSTableReader::scan_for_key_call_count();
        assert!(
            res.is_none(),
            "verification must not change the returned value for a true negative"
        );
        assert!(
            after > before,
            "verification ON must run a confirmation scan through get_with_resolution for the \
             SAME key that showed no scan when OFF"
        );
        let m = mc.flush_and_collect();
        assert_eq!(
            m.counter_sum(catalog::READ_BLOOM_FALSE_NEGATIVES),
            0.0,
            "a true negative reached via get_with_resolution must not emit false_negatives"
        );

        presence_verification::set_enabled_for_testing(false);
        drop(rt);
    }

    // --- Roborev r4 blocker #2 (#2163): `ObservabilityConfig::verify_presence_oracle`
    // must not be decorative. Config-only enablement (NO env var set) must flip
    // the runtime switch through the PUBLIC `observability::init` entry point, and
    // that flip must drive an ACTUAL confirmation scan through the public
    // `get_with_resolution` read path — not merely make `enabled()` report `true`
    // in isolation. Also pins the documented env-overrides-config precedence. ---
    {
        use cqlite_core::observability::ObservabilityConfig;
        use cqlite_core::storage::sstable::reader::SSTableReader;
        use cqlite_core::types::RowKey;

        // Isolate from any real process env for this assertion.
        let saved_env = std::env::var(presence_verification::ENV_VAR).ok();
        std::env::remove_var(presence_verification::ENV_VAR);

        let (reader, table_id, _tmp) = fixtures::open_writer_reader_with_many_rows(30);
        let rt = tokio::runtime::Runtime::new().expect("rt");

        // Locate an in-range absent key (else C5 would short-circuit before the
        // verify hook regardless of the switch) using the TEST setter purely to
        // search — the switch is driven exclusively through `observability::init`
        // for every assertion below.
        presence_verification::set_enabled_for_testing(true);
        let mut in_range_absent_key: Option<[u8; 4]> = None;
        for candidate in 3_000_000i32..3_000_064 {
            let key_bytes = candidate.to_be_bytes();
            let before = SSTableReader::scan_for_key_call_count();
            let res = rt
                .block_on(reader.get_with_resolution(
                    &table_id,
                    &RowKey::from(key_bytes.to_vec()),
                    false,
                ))
                .expect("get on candidate probe");
            let after = SSTableReader::scan_for_key_call_count();
            assert!(
                res.is_none(),
                "candidate {candidate} must be genuinely absent"
            );
            if after == before + 1 {
                in_range_absent_key = Some(key_bytes);
                break;
            }
        }
        let absent_key = in_range_absent_key
            .expect("at least one of 64 candidates must land within the Summary bound");

        // Config-only DISABLE (no env): the switch must be OFF and the same key
        // must show no confirmation scan through the public read path.
        let cfg_off = ObservabilityConfig::builder()
            .enabled(false)
            .verify_presence_oracle(false)
            .build();
        let _guard = cqlite_core::observability::init(cfg_off).expect("init config-off");
        assert!(
            !presence_verification::enabled(),
            "config-only disable must flip the switch off through observability::init"
        );
        let before = SSTableReader::scan_for_key_call_count();
        let res = rt
            .block_on(reader.get_with_resolution(
                &table_id,
                &RowKey::from(absent_key.to_vec()),
                false,
            ))
            .expect("get config-off");
        let after = SSTableReader::scan_for_key_call_count();
        assert!(res.is_none());
        assert_eq!(
            after, before,
            "config-only disable must not run a confirmation scan through get_with_resolution"
        );

        // Config-only ENABLE (no env): the switch must flip ON AND drive an ACTUAL
        // confirmation scan through the public get_with_resolution() path for the
        // SAME key — the end-to-end proof the field is wired, not decorative.
        let cfg_on = ObservabilityConfig::builder()
            .enabled(false)
            .verify_presence_oracle(true)
            .build();
        let _guard = cqlite_core::observability::init(cfg_on).expect("init config-on");
        assert!(
            presence_verification::enabled(),
            "config-only enable must flip the switch on through observability::init"
        );
        let before = SSTableReader::scan_for_key_call_count();
        let res = rt
            .block_on(reader.get_with_resolution(
                &table_id,
                &RowKey::from(absent_key.to_vec()),
                false,
            ))
            .expect("get config-on");
        let after = SSTableReader::scan_for_key_call_count();
        assert!(res.is_none());
        assert!(
            after > before,
            "config-only enable must drive a confirmation scan through get_with_resolution — \
             proving ObservabilityConfig::verify_presence_oracle is wired end-to-end, not \
             decorative"
        );

        // An explicitly-set env var must override a conflicting config value
        // (documented env-overrides-config precedence).
        std::env::set_var(presence_verification::ENV_VAR, "false");
        let cfg_conflict = ObservabilityConfig::builder()
            .enabled(false)
            .verify_presence_oracle(true) // config says ON; env says off
            .build();
        let _guard = cqlite_core::observability::init(cfg_conflict).expect("init env-override");
        assert!(
            !presence_verification::enabled(),
            "an explicitly-set env var must override a conflicting config value"
        );

        // Restore env + leave the switch OFF for any later blocks.
        match saved_env {
            Some(v) => std::env::set_var(presence_verification::ENV_VAR, v),
            None => std::env::remove_var(presence_verification::ENV_VAR),
        }
        presence_verification::set_enabled_for_testing(false);
        drop(rt);
    }

    // --- Roborev r4 item 3 (LOW, folded in, #2163): no REDUNDANT double scan.
    // When the PRIMARY path itself already ran the authoritative `scan_for_key`
    // (a bloom false positive falling through to an Index.db miss), the key was
    // NOT excluded by the presence oracle (`oracle_pruned = false`), so turning
    // verification ON must NOT trigger a second confirmation scan — the delta
    // must stay exactly 1 (the primary scan only) whether verification is OFF or
    // ON, distinguishing "oracle negative" (needs verifying) from "already an
    // authoritative scan result" (nothing left to verify). ---
    {
        use cqlite_core::storage::sstable::reader::SSTableReader;
        use cqlite_core::types::RowKey;

        let (reader, table_id, _tmp) = fixtures::open_writer_reader_with_many_rows(30);
        let rt = tokio::runtime::Runtime::new().expect("rt");

        // Search for a bloom FALSE POSITIVE: `might_contain_partition == true` for
        // a key genuinely absent (well outside the inserted 0..30 range). Default
        // `fp_chance` is 0.01, so finding >=1 among 10,000 candidates is
        // overwhelmingly likely (searched, not assumed).
        let mut fp_key: Option<[u8; 4]> = None;
        for candidate in 4_000_000i32..4_010_000i32 {
            let key_bytes = candidate.to_be_bytes();
            if reader.might_contain_partition(&key_bytes) {
                fp_key = Some(key_bytes);
                break;
            }
        }

        if let Some(fp_key) = fp_key {
            // Verification OFF: the primary path's OWN Index.db-miss fallback
            // scans once (the natural #1572 behaviour, unrelated to the switch).
            presence_verification::set_enabled_for_testing(false);
            let before = SSTableReader::scan_for_key_call_count();
            let res = rt
                .block_on(reader.get_with_resolution(
                    &table_id,
                    &RowKey::from(fp_key.to_vec()),
                    false,
                ))
                .expect("get on bloom-false-positive key, verification off");
            let after = SSTableReader::scan_for_key_call_count();
            assert!(res.is_none(), "the bloom-FP key must still resolve absent");
            assert_eq!(
                after,
                before + 1,
                "the primary path's own scan_for_key fallback must run exactly once"
            );

            // Verification ON, the SAME key: `oracle_pruned` is false (the bloom
            // hit admitted this SSTable; the index miss forced the primary path's
            // OWN scan), so the opt-in verify hook must NOT run a second scan.
            presence_verification::set_enabled_for_testing(true);
            mc.reset();
            let before = SSTableReader::scan_for_key_call_count();
            let res = rt
                .block_on(reader.get_with_resolution(
                    &table_id,
                    &RowKey::from(fp_key.to_vec()),
                    false,
                ))
                .expect("get on bloom-false-positive key, verification on");
            let after = SSTableReader::scan_for_key_call_count();
            assert!(res.is_none(), "the bloom-FP key must still resolve absent");
            assert_eq!(
                after,
                before + 1,
                "verification ON must NOT add a second scan when the primary path already ran \
                 the authoritative scan_for_key (oracle_pruned=false) — exactly one scan total"
            );
            let m = mc.flush_and_collect();
            assert_eq!(
                m.counter_sum(catalog::READ_SSTABLES_PRUNED),
                0.0,
                "a bloom hit (even a false positive) is not a presence-oracle exclusion — must \
                 not emit sstables_pruned"
            );
            assert_eq!(
                m.counter_sum(catalog::READ_BLOOM_FALSE_NEGATIVES),
                0.0,
                "the skipped (non-oracle) case must never emit false_negatives"
            );
            presence_verification::set_enabled_for_testing(false);
        } else {
            eprintln!(
                "correctness_signals_2163_verify: no bloom false positive found among 10,000 \
                 candidates (fp_chance=0.01 makes this astronomically unlikely) — skipping the \
                 no-double-scan sub-assertion for this run; the structural invariant (verify \
                 only runs when oracle_pruned) is still enforced by the code path itself"
            );
        }
        drop(rt);
    }

    // --- Roborev r5 (#2163): the opt-in verify scan's `Err` must not be
    // silently discarded. The READ stays fail-open (a verifier failure must
    // never fail the actual read), but the failure must be surfaced LOUDLY:
    // recorded through the EXISTING error-signal path
    // (`cqlite.errors.total{subsystem=reader}`), never a new metric. ---
    {
        use cqlite_core::types::RowKey;

        let (reader, table_id, data_path, _tmp) =
            fixtures::open_writer_reader_with_many_rows_and_path(30);

        // Find an in-range absent key. `might_contain_partition` only consults
        // the ALREADY-LOADED (resident) Filter.db, so this search — and the
        // primary oracle-negative it proves — is unaffected by the Data.db
        // corruption applied below.
        let mut absent_key: Option<[u8; 4]> = None;
        for candidate in 5_000_000i32..5_000_064i32 {
            let key_bytes = candidate.to_be_bytes();
            if !reader.might_contain_partition(&key_bytes) {
                absent_key = Some(key_bytes);
                break;
            }
        }
        let absent_key = absent_key
            .expect("at least one candidate must show absent via the resident bloom filter");

        // Corrupt Data.db AFTER open (truncate mid-file): the resident
        // Filter.db is untouched (lives in memory), so the primary bloom-miss
        // path still succeeds; but the opt-in verify's authoritative
        // `scan_for_key` — which must read Data.db fresh — now fails.
        let original_len = std::fs::metadata(&data_path).expect("stat Data.db").len();
        assert!(
            original_len > 8,
            "the fixture's Data.db must be large enough to truncate meaningfully"
        );
        let truncated_len = (original_len / 2).max(1);
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_path)
            .expect("open Data.db for truncation");
        file.set_len(truncated_len)
            .expect("truncate Data.db to simulate corruption/an unreadable SSTable");
        drop(file);

        presence_verification::set_enabled_for_testing(true);
        mc.reset();
        let rt = tokio::runtime::Runtime::new().expect("rt");
        let res = rt
            .block_on(reader.get_with_resolution(
                &table_id,
                &RowKey::from(absent_key.to_vec()),
                false,
            ))
            .expect("get_with_resolution must stay fail-open despite a verify-scan failure");
        drop(rt);
        presence_verification::set_enabled_for_testing(false);

        assert!(
            res.is_none(),
            "the read must return the ORIGINAL oracle-negative result unaffected by the \
             verify-scan failure (fail-open) — a debug/soundness check failing must never break \
             a production read"
        );

        let m = mc.flush_and_collect();
        let recorded = m.sum_where(
            catalog::ERRORS_TOTAL,
            &[(catalog::attr::SUBSYSTEM, "reader")],
        );
        assert!(
            recorded >= 1.0,
            "the verify-scan failure must be recorded through the existing error-signal path \
             (cqlite.errors.total{{subsystem=reader}}) — a silent-miss DETECTOR must not itself \
             fail silently; entry: {:?}",
            m.find(catalog::ERRORS_TOTAL)
        );
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
// Reader fixtures (writer-produced, schema-attached readers for the verify
// switch scenarios).
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

    /// Write `n` rows (ids `0..n`), flush, and open a schema-attached reader over
    /// the produced Data.db. Used by the `get()`-level false-negative wiring test
    /// (roborev fold-in #3, #2163), which needs an absent probe key whose token
    /// falls WITHIN the reader's authoritative `[first_key, last_key]` Summary
    /// bound (else the C5 range short-circuit in `get_with_resolution` returns
    /// absence before ever reaching the presence-oracle / verify hook). `n` rows
    /// spread the observed token span wide enough that a handful of far-outside-
    /// range candidate ids (searched by the caller) are overwhelmingly likely to
    /// land in-range by token, even though token order is unrelated to int order.
    pub fn open_writer_reader_with_many_rows(
        n: i32,
    ) -> (SSTableReader, TableId, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let schema = writer_schema();
        let cfg = WriteEngineConfig::new(
            tmp.path().join("data"),
            tmp.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(cfg).expect("engine");
        for id in 0..n {
            engine
                .execute(&format!(
                    "INSERT INTO obsfn.rows (id, name) VALUES ({id}, 'row{id}')"
                ))
                .expect("write");
        }

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
        drop(rt);

        (reader, TableId::from("rows"), tmp)
    }

    /// Like [`open_writer_reader_with_many_rows`], but also returns the on-disk
    /// `Data.db` path so a test can corrupt it AFTER the reader has already
    /// loaded Filter.db/Index.db/Summary.db into memory (roborev r5, #2163's
    /// verify-scan-error test) — the presence-oracle bloom check never re-reads
    /// Data.db, so corrupting the file post-open leaves the primary oracle
    /// negative intact while making a SUBSEQUENT `scan_for_key` (the opt-in
    /// verify path) fail.
    pub fn open_writer_reader_with_many_rows_and_path(
        n: i32,
    ) -> (SSTableReader, TableId, PathBuf, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().expect("tmp");
        let schema = writer_schema();
        let cfg = WriteEngineConfig::new(
            tmp.path().join("data"),
            tmp.path().join("wal"),
            schema.clone(),
        );
        let mut engine = WriteEngine::new(cfg).expect("engine");
        for id in 0..n {
            engine
                .execute(&format!(
                    "INSERT INTO obsfn.rows (id, name) VALUES ({id}, 'row{id}')"
                ))
                .expect("write");
        }

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
        drop(rt);

        (reader, TableId::from("rows"), data_file, tmp)
    }
}
