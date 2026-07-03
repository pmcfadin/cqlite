//! Compaction / k-way-merge micro-benchmarks for cqlite-core (Issue #1646,
//! Epic O — write measurement, finding O1).
//!
//! The k-way merge (`storage::write_engine::merge`) is the heaviest CPU in the
//! write path and drives every compaction, yet it had zero bench coverage.
//! These benches measure a full multi-generation STCS compaction — flush
//! `min_threshold` L0 SSTables (untimed setup) then drive
//! `WriteEngine::maintenance_step` to completion (timed) — across three shapes:
//!
//! - `compaction/narrow` — many small partitions, few rows each. The CPU-bound
//!   merge-core probe (one row per partition, `UUID` PK, no clustering).
//!   **Strictly gated** in `perf-gate.json`.
//!
//! - `compaction/wide` — a few fat partitions with many clustering rows,
//!   contributed disjointly by each input SSTable so the merged partition is the
//!   union of all of them (exercises the wide-partition merge path). It is
//!   memory/data-dependent, so it is tracked **advisory** — O2 owns its dhat
//!   budget, not a wall-clock gate.
//!
//! - `compaction/tombstone_heavy` — live rows shadowed by row/range/cell
//!   tombstones in a later generation, so the reconcile + range-shadowing path
//!   is exercised. **Strictly gated** in `perf-gate.json`.
//!
//! The merge policy is installed **explicitly** in the bench via
//! `set_merge_policy(STCSPolicy::default())`, so O1 measures compaction
//! regardless of whether the default-on STCS wiring (N1) has landed.
//!
//! All input is generated from [`fixtures::seeded_rng`] so the compacted data is
//! byte-for-byte identical across runs and machines, and each iteration uses a
//! fresh [`tempfile::TempDir`] so iterations never share state.
//!
//! ## Running
//!
//! ```text
//! # default features (compiles as a no-op group):
//! cargo bench -p cqlite-core --bench compaction -- --test
//!
//! # with write-support:
//! cargo bench -p cqlite-core --features write-support --bench compaction -- --test
//! cargo bench -p cqlite-core --features write-support --bench compaction
//! ```

use criterion::{criterion_group, criterion_main, Criterion};

#[cfg(feature = "write-support")]
use criterion::{black_box, Throughput};

#[path = "fixtures/mod.rs"]
mod fixtures;

#[path = "profiling/mod.rs"]
mod profiling;

// ── Shape sizing (feature-gated so the no-op build stays clean) ──────────────

/// Number of L0 SSTables flushed before each compaction. `>=` the STCS default
/// `min_threshold` (4) so the policy selects a bucket. The tiny bench SSTables
/// are all below `min_sstable_size` (50 MB), so STCS groups them into a single
/// eligible bucket — a full compaction of every input.
#[cfg(feature = "write-support")]
const L0_SSTABLES: usize = 4;

/// `compaction/narrow`: partitions per flushed SSTable (one row each). Disjoint
/// keys across SSTables, so the merge output is the sum of all inputs.
#[cfg(feature = "write-support")]
const NARROW_ROWS_PER_TABLE: usize = 200;

/// `compaction/wide`: number of fat partitions.
#[cfg(feature = "write-support")]
const WIDE_PARTITIONS: usize = 2;

/// `compaction/wide`: clustering rows each SSTable contributes to every
/// partition (disjoint clustering ranges per SSTable so the merged partition is
/// the union of all `L0_SSTABLES` slices — a genuinely fat merged partition).
#[cfg(feature = "write-support")]
const WIDE_CK_PER_TABLE: usize = 150;

/// `compaction/tombstone_heavy`: number of partitions carrying live+shadowed rows.
#[cfg(feature = "write-support")]
const TOMB_PARTITIONS: usize = 2;

/// `compaction/tombstone_heavy`: clustering rows per partition (the SAME keys in
/// every live SSTable so the reconcile path runs), of which the lower half is
/// shadowed by a range/row/cell tombstone in the final generation.
#[cfg(feature = "write-support")]
const TOMB_CK: usize = 150;

/// CQL for the wide / tombstone bench target: a `(pk, ck)` composite primary key
/// so a single partition holds many clustering rows. Single `CREATE TABLE` so
/// the no-heuristics mandate (Issue #28) has an unambiguous write target.
#[cfg(feature = "write-support")]
const WIDE_TABLE_CQL: &str = "\
CREATE TABLE test_bench.wide_table (
    pk INT,
    ck INT,
    val TEXT,
    PRIMARY KEY (pk, ck)
);";

// ── Shared helpers ──────────────────────────────────────────────────────────

/// Build a `WriteEngine` over `cql` whose data/WAL dirs live under `dir`, with a
/// huge flush threshold so no auto-flush fires mid-batch (the bench flushes
/// explicitly to control the number of L0 SSTables).
#[cfg(feature = "write-support")]
fn open_engine(
    dir: &std::path::Path,
    cql: &str,
    flush_threshold: usize,
) -> cqlite_core::storage::write_engine::WriteEngine {
    use cqlite_core::schema::parse_cql_schema;
    use cqlite_core::storage::write_engine::{WriteEngine, WriteEngineConfig};

    let schema = parse_cql_schema(cql).expect("parse compaction-bench schema");
    let cfg = WriteEngineConfig::new(dir.join("data"), dir.join("wal"), schema)
        .with_flush_threshold(flush_threshold);
    WriteEngine::new(cfg).expect("build compaction-bench write engine")
}

/// Install the default STCS policy (`min_threshold = 4`) explicitly, so the
/// bench measures compaction independent of N1's default-on wiring.
#[cfg(feature = "write-support")]
fn install_stcs(engine: &mut cqlite_core::storage::write_engine::WriteEngine) {
    use cqlite_core::storage::write_engine::STCSPolicy;
    engine
        .set_merge_policy(Box::new(STCSPolicy::default()))
        .expect("install STCS merge policy");
}

/// Drive `maintenance_step` until no compaction work remains, returning the
/// total rows merged (output rows). Returns `> 0` iff a real compaction ran.
#[cfg(feature = "write-support")]
fn drive_compaction_to_completion(
    engine: &mut cqlite_core::storage::write_engine::WriteEngine,
) -> u64 {
    use std::time::Duration;
    // A near-unbounded budget so each step completes its active merge in one
    // call. NOT `Duration::MAX`: `maintenance_step` computes `budget * 1.1` for
    // its tolerance window, and `Duration::MAX * 1.1` overflows and panics. One
    // day is unbounded for a bench-sized merge while leaving headroom for ×1.1.
    let budget = Duration::from_secs(86_400);
    let mut total = 0u64;
    loop {
        let report = engine
            .maintenance_step(budget)
            .expect("compaction maintenance step");
        total += report.rows_merged;
        // Terminate once the policy selects nothing more AND no merge is active:
        // a finished merge reports `rows_merged > 0` with `pending == false`, so
        // we loop once more to let the policy re-check the (now fewer) SSTables.
        if !report.pending_compaction && report.rows_merged == 0 {
            break;
        }
    }
    total
}

// ── compaction/narrow ────────────────────────────────────────────────────────

/// Build `L0_SSTABLES` SSTables of `NARROW_ROWS_PER_TABLE` single-row partitions
/// (disjoint `UUID` keys), then install STCS. Returns the primed engine.
#[cfg(feature = "write-support")]
fn build_narrow(
    rt: &tokio::runtime::Runtime,
    dir: &std::path::Path,
) -> cqlite_core::storage::write_engine::WriteEngine {
    use rand::Rng;

    let mut engine = fixtures::open_write_engine(dir, usize::MAX);
    let mut rng = fixtures::seeded_rng();
    for _ in 0..L0_SSTABLES {
        for _ in 0..NARROW_ROWS_PER_TABLE {
            let id = uuid::Uuid::from_u128(rng.gen());
            let age: i32 = rng.gen_range(0..100);
            let stmt = format!(
                "INSERT INTO test_basic.simple_table \
                 (id, name, age, active) \
                 VALUES ({id}, 'narrow-row', {age}, true)"
            );
            engine.execute(&stmt).expect("narrow ingest row");
        }
        let flushed = rt
            .block_on(engine.flush())
            .expect("narrow flush must not error");
        assert!(flushed.is_some(), "narrow flush produced no SSTable");
    }
    install_stcs(&mut engine);
    engine
}

/// `compaction/narrow` — CPU-bound merge core over many small partitions.
#[cfg(feature = "write-support")]
fn bench_narrow(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime for narrow setup");
    let input_rows = (L0_SSTABLES * NARROW_ROWS_PER_TABLE) as u64;

    let mut group = c.benchmark_group("compaction");
    group.throughput(Throughput::Elements(input_rows));
    group.bench_function("narrow", |b| {
        b.iter_batched(
            // SETUP (untimed): flush L0 SSTables + install STCS.
            || {
                let tmp = tempfile::TempDir::new().expect("temp dir for narrow bench");
                let engine = build_narrow(&rt, tmp.path());
                (tmp, engine)
            },
            // ROUTINE (timed): compact all L0 SSTables to completion.
            |(_tmp, mut engine)| {
                let merged = drive_compaction_to_completion(&mut engine);
                assert!(
                    merged > 0,
                    "narrow: compaction merged 0 rows — no real work ran"
                );
                black_box(merged)
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ── compaction/wide ───────────────────────────────────────────────────────────

/// Build `L0_SSTABLES` SSTables that each contribute a DISJOINT clustering slice
/// to the same `WIDE_PARTITIONS` partitions, so the merged partitions are fat
/// (the union of every slice). Returns the primed engine.
#[cfg(feature = "write-support")]
fn build_wide(
    rt: &tokio::runtime::Runtime,
    dir: &std::path::Path,
) -> cqlite_core::storage::write_engine::WriteEngine {
    use cqlite_core::storage::write_engine::{
        CellOperation, ClusteringKey, Mutation, PartitionKey, TableId,
    };
    use cqlite_core::types::Value;
    use rand::Rng;

    let mut engine = open_engine(dir, WIDE_TABLE_CQL, usize::MAX);
    let mut rng = fixtures::seeded_rng();
    let tid = TableId::new("test_bench", "wide_table");
    for t in 0..L0_SSTABLES {
        for pk in 0..WIDE_PARTITIONS {
            for c in 0..WIDE_CK_PER_TABLE {
                // Disjoint clustering key per SSTable → fat merged partition.
                let ck = (t * WIDE_CK_PER_TABLE + c) as i32;
                let val: u32 = rng.gen();
                let m = Mutation::new(
                    tid.clone(),
                    PartitionKey::single("pk", Value::Integer(pk as i32)),
                    Some(ClusteringKey::single("ck", Value::Integer(ck))),
                    vec![CellOperation::Write {
                        column: "val".to_string(),
                        value: Value::Text(format!("wide-{val}")),
                    }],
                    1_000_000 + ck as i64,
                    None,
                );
                engine.write(m).expect("wide write row");
            }
        }
        let flushed = rt
            .block_on(engine.flush())
            .expect("wide flush must not error");
        assert!(flushed.is_some(), "wide flush produced no SSTable");
    }
    install_stcs(&mut engine);
    engine
}

/// `compaction/wide` — advisory (memory/data-shaped): fat-partition merge.
#[cfg(feature = "write-support")]
fn bench_wide(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime for wide setup");
    let input_rows = (L0_SSTABLES * WIDE_PARTITIONS * WIDE_CK_PER_TABLE) as u64;

    let mut group = c.benchmark_group("compaction");
    group.throughput(Throughput::Elements(input_rows));
    group.bench_function("wide", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::TempDir::new().expect("temp dir for wide bench");
                let engine = build_wide(&rt, tmp.path());
                (tmp, engine)
            },
            |(_tmp, mut engine)| {
                let merged = drive_compaction_to_completion(&mut engine);
                assert!(
                    merged > 0,
                    "wide: compaction merged 0 rows — no real work ran"
                );
                black_box(merged)
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ── compaction/tombstone_heavy ─────────────────────────────────────────────────

/// Build live rows in the first `L0_SSTABLES - 1` SSTables (same clustering keys
/// so the reconcile path runs), then a final SSTable of row/range/cell
/// tombstones shadowing the LOWER half of the clustering range. The upper half
/// survives, so the merge does real reconcile + range-shadowing work and still
/// emits output. Returns the primed engine.
#[cfg(feature = "write-support")]
fn build_tombstone_heavy(
    rt: &tokio::runtime::Runtime,
    dir: &std::path::Path,
) -> cqlite_core::storage::write_engine::WriteEngine {
    use cqlite_core::storage::write_engine::{
        CellOperation, ClusteringBound, ClusteringKey, Mutation, PartitionKey, RangeTombstone,
        TableId,
    };
    use cqlite_core::types::Value;
    use rand::Rng;

    let mut engine = open_engine(dir, WIDE_TABLE_CQL, usize::MAX);
    let mut rng = fixtures::seeded_rng();
    let tid = TableId::new("test_bench", "wide_table");
    let half = (TOMB_CK / 2) as i32;

    // Live generations: the same (pk, ck) keys in every SSTable so compaction
    // reconciles overlapping cells across generations.
    for gen in 0..(L0_SSTABLES - 1) {
        for pk in 0..TOMB_PARTITIONS {
            for ck in 0..TOMB_CK {
                let val: u32 = rng.gen();
                let m = Mutation::new(
                    tid.clone(),
                    PartitionKey::single("pk", Value::Integer(pk as i32)),
                    Some(ClusteringKey::single("ck", Value::Integer(ck as i32))),
                    vec![CellOperation::Write {
                        column: "val".to_string(),
                        value: Value::Text(format!("live-{gen}-{val}")),
                    }],
                    1_000_000 + (gen * TOMB_CK + ck) as i64,
                    None,
                );
                engine.write(m).expect("tombstone-heavy live write");
            }
        }
        let flushed = rt
            .block_on(engine.flush())
            .expect("tombstone-heavy live flush must not error");
        assert!(
            flushed.is_some(),
            "tombstone-heavy live flush produced no SSTable"
        );
    }

    // Tombstone generation: a higher timestamp so it shadows the live rows.
    // Covers the lower clustering half via a range tombstone, plus a row
    // tombstone and a cell tombstone inside that range (the reconcile path must
    // resolve all three against the live cells).
    let tomb_ts = 1_000_000 + (L0_SSTABLES * TOMB_CK) as i64;
    let tomb_ldt = (tomb_ts / 1_000_000) as i32;
    for pk in 0..TOMB_PARTITIONS {
        let pk_val = Value::Integer(pk as i32);

        // Range tombstone: [Bottom, ck = half) shadows the lower half.
        let mut range_mut = Mutation::new(
            tid.clone(),
            PartitionKey::single("pk", pk_val.clone()),
            None,
            vec![],
            tomb_ts,
            None,
        );
        range_mut.range_tombstones.push(RangeTombstone {
            start: ClusteringBound::Bottom,
            end: ClusteringBound::Exclusive(ClusteringKey::single("ck", Value::Integer(half))),
            deletion_time: tomb_ts,
            local_deletion_time: tomb_ldt,
        });
        engine.write(range_mut).expect("range tombstone write");

        // Row tombstone on a single clustering key inside the shadowed range.
        let row_mut = Mutation::new(
            tid.clone(),
            PartitionKey::single("pk", pk_val.clone()),
            Some(ClusteringKey::single("ck", Value::Integer(0))),
            vec![CellOperation::DeleteRow],
            tomb_ts,
            None,
        );
        engine.write(row_mut).expect("row tombstone write");

        // Cell tombstone on the `val` column of another key inside the range.
        let cell_mut = Mutation::new(
            tid.clone(),
            PartitionKey::single("pk", pk_val),
            Some(ClusteringKey::single("ck", Value::Integer(1))),
            vec![CellOperation::Delete {
                column: "val".to_string(),
                local_deletion_time: None,
            }],
            tomb_ts,
            None,
        );
        engine.write(cell_mut).expect("cell tombstone write");
    }
    let flushed = rt
        .block_on(engine.flush())
        .expect("tombstone flush must not error");
    assert!(flushed.is_some(), "tombstone flush produced no SSTable");

    install_stcs(&mut engine);
    engine
}

/// `compaction/tombstone_heavy` — CPU-bound reconcile + range-shadowing path.
#[cfg(feature = "write-support")]
fn bench_tombstone_heavy(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime for tombstone setup");
    // Denominator = total LIVE input rows fed into the merge (the reconcile work).
    let input_rows = ((L0_SSTABLES - 1) * TOMB_PARTITIONS * TOMB_CK) as u64;

    let mut group = c.benchmark_group("compaction");
    group.throughput(Throughput::Elements(input_rows));
    group.bench_function("tombstone_heavy", |b| {
        b.iter_batched(
            || {
                let tmp = tempfile::TempDir::new().expect("temp dir for tombstone bench");
                let engine = build_tombstone_heavy(&rt, tmp.path());
                (tmp, engine)
            },
            |(_tmp, mut engine)| {
                let merged = drive_compaction_to_completion(&mut engine);
                assert!(
                    merged > 0,
                    "tombstone_heavy: compaction merged 0 surviving rows — \
                     tombstones shadowed everything (or no work ran)"
                );
                black_box(merged)
            },
            criterion::BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ── criterion_group! / criterion_main! ──────────────────────────────────────
//
// Two variants mirror write.rs so the file compiles under every feature
// combination without dead-code noise: real benches with `write-support`, a
// single no-op group without it.

#[cfg(feature = "write-support")]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_narrow, bench_wide, bench_tombstone_heavy
);

#[cfg(not(feature = "write-support"))]
fn bench_noop(_c: &mut Criterion) {
    // write-support is disabled; these benches are no-ops.
    // Enable with: --features write-support
}

#[cfg(not(feature = "write-support"))]
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_noop
);

criterion_main!(benches);
