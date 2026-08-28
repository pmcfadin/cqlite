//! Issue #1697 (AH4 — config source of truth): setting a knob on the PUBLIC
//! [`Config`] surface must change what the write engine actually does.
//!
//! CQLite carried two independent config layers for the write path: the public
//! `Config.storage.*` facade an embedder can set, and the private
//! `WriteEngineConfig` the engine actually reads. Three live knobs had no public
//! route into the engine at all:
//!
//! | knob | private (live) | public (decorative) |
//! |------|----------------|---------------------|
//! | memtable flush threshold | `WriteEngineConfig::memtable_flush_threshold` (64MB) | `Config.storage.memtable_size_threshold` (was 16MB, ZERO compiled readers) |
//! | memtable hard limit | `WriteEngineConfig::memtable_hard_limit` (256MB) | none |
//! | STCS `min_threshold` | `WriteEngineConfig::compaction_min_threshold` (4) | none |
//! | STCS `max_threshold` | `WriteEngineConfig::compaction_max_threshold` (32) | none |
//!
//! So an embedder could set `memtable_size_threshold = 4KB` and get 64MB
//! behaviour, silently — and could be hard-failed by a 256MB admission ceiling
//! they had no way to see. (The two files that appeared to read
//! `memtable_size_threshold` — `tests/integration/performance_integration.rs`
//! and `tests/benchmarks/load_testing.rs` — are not registered cargo targets
//! and never compile, so the reader count was an absolute zero; the guards
//! below are the first compiled readers.) Each guard drives the PUBLIC field
//! and asserts an OBSERVABLE engine effect (an SSTable generation appearing, a
//! write rejected, rows merged, merge width), never elapsed time.
//!
//! [`public_auto_compaction_off_disables_compaction`] is the regression anchor
//! for the ONE knob that was already wired (issue #1619 / N1): it must pass
//! before and after this change, proving the new single bridge did not break it.
//!
//! ## The bridge under test
//!
//! [`engine_config`] below names the one thing being changed: the translation
//! from a public [`Config`] to the engine's private config. Every assertion in
//! this file is written against that translation, not against a particular
//! spelling of it, so the RED->GREEN transition is a change of BRIDGE, not a
//! change of expectations.
//!
//! Run with:
//!   cargo test --package cqlite-core --features write-support \
//!     --test issue_1697_config_single_source

#![cfg(feature = "write-support")]

use cqlite_core::schema::{Column, KeyColumn, TableSchema};
use cqlite_core::storage::write_engine::{
    CellOperation, Mutation, PartitionKey, TableId, WriteEngine, WriteEngineConfig,
};
use cqlite_core::types::Value;
use cqlite_core::Config;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tempfile::TempDir;

const KS: &str = "cfg_ks";
const TBL: &str = "knobs";

/// Build the write-engine config from the PUBLIC [`Config`] surface.
///
/// This is the single seam issue #1697 changes. Before the fix, the only
/// public-`Config` route into the engine was `with_compaction_config`, which
/// threaded `auto_compaction` and NOTHING else — every other public knob was
/// dropped on the floor here. After the fix this delegates to the one canonical
/// bridge, `WriteEngineConfig::from_config`.
fn engine_config(
    config: &Config,
    data_dir: PathBuf,
    wal_dir: PathBuf,
    schema: TableSchema,
) -> WriteEngineConfig {
    WriteEngineConfig::from_config(config, data_dir, wal_dir, schema)
}

fn make_schema() -> TableSchema {
    TableSchema {
        keyspace: KS.to_string(),
        table: TBL.to_string(),
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
                name: "payload".to_string(),
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

/// A row whose `payload` is `payload_len` bytes, so a test can drive the
/// memtable past a byte threshold with a known, bounded number of writes.
fn row(id: i32, payload_len: usize, ts: i64) -> Mutation {
    let pk = PartitionKey::single("id", Value::Integer(id));
    let ops = vec![CellOperation::Write {
        column: "payload".to_string(),
        value: Value::text("x".repeat(payload_len)),
    }];
    Mutation::new(TableId::new(KS, TBL), pk, None, ops, ts, None)
}

fn count_data_files(dir: &Path) -> usize {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with("-Data.db"))
            .count(),
        // Not created yet == no SSTables flushed.
        Err(_) => 0,
    }
}

/// Flush `n` distinct L0 SSTables of comparable size, so STCS sees them as one
/// bucket. Mirrors the crate-internal `flush_n_sstables_sync` test helper, which
/// integration tests cannot reach (`pub(crate)`).
fn flush_n_sstables(engine: &mut WriteEngine, n: usize) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    for batch in 0..n {
        for r in 0..5 {
            let id = (batch * 100 + r) as i32;
            engine
                .write(row(id, 32, 1_000_000 + id as i64))
                .expect("write");
        }
        rt.block_on(engine.flush())
            .expect("flush")
            .expect("flush must produce an SSTable");
    }
}

// ───────────────────────── memtable flush threshold ─────────────────────────

/// AC: `Config.storage.memtable_size_threshold` must be the value that triggers
/// a flush. Set it to 4KB and write well past 4KB; an SSTable must appear.
///
/// RED (before the bridge): the public field is ignored and the private 64MB
/// default runs, so nothing is ever flushed — `generation()` stays at 0 and the
/// table directory holds no Data.db.
#[test]
fn public_memtable_threshold_drives_flush() {
    const THRESHOLD: u64 = 4096;

    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");

    let mut config = Config::default();
    config.storage.memtable_size_threshold = THRESHOLD;

    let mut engine = WriteEngine::new(engine_config(
        &config,
        data_dir.clone(),
        wal_dir,
        make_schema(),
    ))
    .expect("engine creation");

    let gen_before = engine.generation();

    // 200 rows x 256-byte payload ~= 51KB of live data, >>4KB. Written OUTSIDE a
    // Tokio runtime so the engine's sync auto-flush path is the one under test.
    for id in 0..200i32 {
        engine
            .write(row(id, 256, 1_000_000 + id as i64))
            .expect("write");
    }

    let table_dir = data_dir.join(KS).join(TBL);
    let files = count_data_files(&table_dir);

    assert!(
        files > 0,
        "public memtable_size_threshold = {THRESHOLD} B must trigger a flush after ~51KB of \
         writes, but no Data.db exists under {table_dir:?} (the public knob never reached the \
         engine; the private 64MB default ran instead)"
    );
    assert!(
        engine.generation() > gen_before,
        "generation must advance past {gen_before} once the public threshold flushes \
         (observed {})",
        engine.generation()
    );
    // The flush must happen at approximately the configured size, not 64MB: the
    // memtable is bounded by roughly one threshold's worth of residue.
    assert!(
        (engine.memtable_size() as u64) < 16 * THRESHOLD,
        "memtable must stay near the configured {THRESHOLD} B threshold after auto-flush \
         (observed {} B)",
        engine.memtable_size()
    );
}

/// AC1: `Config.storage.memtable_hard_limit` must be the admission ceiling the
/// engine's `check_admission` enforces. Set it to 1KB and submit a single ~4KB
/// mutation: the write must be REJECTED, and the error must quote the CONFIGURED
/// limit (not 256MB), which is what proves the public value is the one in force.
///
/// RED (before the bridge): no public field exists to set, so the private 256MB
/// default runs and a 4KB mutation is admitted without complaint.
///
/// Named for the established `*_knob_is_load_bearing` convention
/// (`dead_cache_delete_tests.rs`, `issue_1582_byte_bounded_result_budget.rs`).
#[test]
fn public_memtable_hard_limit_knob_is_load_bearing() {
    const HARD_LIMIT: u64 = 1024;

    let temp = TempDir::new().expect("tempdir");
    let mut config = Config::default();
    // Below the ceiling, so `Config::validate` is satisfied (a hard limit under
    // the flush threshold would wedge the engine and is rejected outright).
    config.storage.memtable_size_threshold = 512;
    config.storage.memtable_hard_limit = HARD_LIMIT;
    config
        .validate()
        .expect("config must be internally consistent");

    let mut engine = WriteEngine::new(engine_config(
        &config,
        temp.path().join("data"),
        temp.path().join("wal"),
        make_schema(),
    ))
    .expect("engine creation");

    // One mutation far larger than the configured ceiling.
    let err = engine
        .write(row(1, 4096, 1_000_000))
        .expect_err("a single 4KB mutation must be rejected under a 1KB hard limit");
    let msg = err.to_string();
    assert!(
        msg.contains("hard limit") && msg.contains(&HARD_LIMIT.to_string()),
        "the rejection must quote the CONFIGURED hard limit {HARD_LIMIT}, proving the public knob reached check_admission; got: {msg}"
    );

    // A mutation that fits is still admitted — the knob bounds, it does not brick.
    engine
        .write(row(2, 16, 1_000_001))
        .expect("a small mutation must still be admitted under the same limit");
}

/// `Config::validate` must refuse a hard limit BELOW the flush threshold: such
/// an engine rejects writes at the ceiling before a flush can ever relieve the
/// memtable. Only expressible as a rule now that both knobs live in one struct.
#[test]
fn hard_limit_below_flush_threshold_is_rejected_at_config_time() {
    let mut config = Config::default();
    config.storage.memtable_size_threshold = 8 * 1024;
    config.storage.memtable_hard_limit = 4 * 1024;
    let err = config
        .validate()
        .expect_err("a wedged memtable configuration must not validate");
    assert!(
        err.to_string().contains("memtable_hard_limit"),
        "the error must name the offending knob; got: {err}"
    );
}

// ───────────────────────────── STCS thresholds ──────────────────────────────

/// AC: the public STCS `min_threshold` must change the engine's effective
/// eligibility bar. With the public min set to 2, TWO L0 SSTables must compact.
///
/// RED (before the bridge): no public field exists to set, so the private
/// default 4 runs and two SSTables are never eligible — `rows_merged == 0`.
#[test]
fn public_compaction_thresholds_drive_stcs() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");

    let mut config = Config::default();
    config.storage.compaction.auto_compaction = true;
    config.storage.compaction.min_threshold = 2;
    config.storage.compaction.max_threshold = 32;

    let mut engine = WriteEngine::new(engine_config(
        &config,
        data_dir.clone(),
        wal_dir,
        make_schema(),
    ))
    .expect("engine creation");

    flush_n_sstables(&mut engine, 2);
    let table_dir = data_dir.join(KS).join(TBL);
    let before = count_data_files(&table_dir);
    assert_eq!(before, 2, "test must start from exactly 2 L0 SSTables");

    let report = engine
        .maintenance_step(Duration::from_secs(60))
        .expect("maintenance_step");

    assert!(
        report.rows_merged > 0,
        "public compaction.min_threshold = 2 must make 2 SSTables eligible for STCS \
         (rows_merged = {}); the private default 4 was used instead",
        report.rows_merged
    );
    let after = count_data_files(&table_dir);
    assert!(
        after < before,
        "on-disk SSTable count must drop after the compaction (before = {before}, after = {after})"
    );
}

/// AC: the public STCS `max_threshold` must cap how many SSTables one merge
/// consumes. With min = 2 and max = 2 over FIVE eligible L0 SSTables, one step
/// must merge exactly 2 -> 5 files become 4.
///
/// RED (before the bridge): the private default max of 32 runs, so all 5 are
/// merged into 1.
#[test]
fn public_compaction_max_threshold_caps_merge_width() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");

    let mut config = Config::default();
    config.storage.compaction.auto_compaction = true;
    config.storage.compaction.min_threshold = 2;
    config.storage.compaction.max_threshold = 2;

    let mut engine = WriteEngine::new(engine_config(
        &config,
        data_dir.clone(),
        wal_dir,
        make_schema(),
    ))
    .expect("engine creation");

    flush_n_sstables(&mut engine, 5);
    let table_dir = data_dir.join(KS).join(TBL);
    assert_eq!(
        count_data_files(&table_dir),
        5,
        "test must start from exactly 5 L0 SSTables"
    );

    engine
        .maintenance_step(Duration::from_secs(60))
        .expect("maintenance_step");

    assert_eq!(
        count_data_files(&table_dir),
        4,
        "public compaction.max_threshold = 2 must cap one merge at 2 inputs (5 - 2 + 1 = 4 \
         files); a wider result means the private default 32 was used"
    );
}

// ───────────────── regression anchor: the already-wired knob ─────────────────

/// Regression anchor for issue #1619 (N1): `auto_compaction = false` was ALREADY
/// threaded through the public `Config`. This test must pass BOTH before and
/// after the #1697 bridge, proving consolidation did not break existing wiring.
#[test]
fn public_auto_compaction_off_disables_compaction() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().join("data");
    let wal_dir = temp.path().join("wal");

    let mut config = Config::default();
    config.storage.compaction.auto_compaction = false;

    let mut engine = WriteEngine::new(engine_config(
        &config,
        data_dir.clone(),
        wal_dir,
        make_schema(),
    ))
    .expect("engine creation");

    flush_n_sstables(&mut engine, 4);
    let table_dir = data_dir.join(KS).join(TBL);
    let before = count_data_files(&table_dir);
    assert_eq!(before, 4, "test must start from exactly 4 L0 SSTables");

    let report = engine
        .maintenance_step(Duration::from_secs(60))
        .expect("maintenance_step");

    assert_eq!(
        report.rows_merged, 0,
        "auto_compaction = false must install no policy (rows_merged = {})",
        report.rows_merged
    );
    assert!(
        !report.pending_compaction,
        "auto_compaction = false must report no pending compaction"
    );
    assert_eq!(
        count_data_files(&table_dir),
        before,
        "auto_compaction = false must leave the L0 SSTable count unchanged"
    );
}
