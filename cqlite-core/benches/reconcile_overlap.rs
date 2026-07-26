//! Reconcile **generation-overlap multiplier** bench (issue #2043 / M9, epic #2817).
//!
//! The 0.17 throughput arithmetic (`docs/architecture/throughput-program-2026-07.md`
//! §3) divides the fixed per-core pipeline rate by a field derate of 1.5–3×, inside
//! which sits a **compaction generation-overlap term of ~1.1–1.5×** — the one factor
//! in the chain with no measurement behind it. The reconcile *base* is already
//! measured (~2.0 µs/row on narrow disjoint singleton clusters,
//! `docs/research/phase2-verify-stage2.md:226-232`), but that corpus has **no
//! overlap**, so it is the `k = 1` point of a curve whose slope nobody had measured.
//!
//! This bench measures that curve: per-row merge cost for row clusters spanning
//! **k** SSTable generations, k ∈ {1, 2, 5, 10, 20}, crossed with the collision
//! mixes {`disjoint`, `lww_overwrite`, `tombstone`, `ttl_expiring`, `field_blend`}
//! (see [`fixtures::multigen::OverlapMix`]). The derived multiplier is
//! `cost(k) / cost(1)` per mix; the record is
//! `docs/research/issue-2043-reconcile-overlap-multiplier.md`.
//!
//! ## What is timed, and why it is the whole `KWayMerger` drain
//!
//! Each iteration builds a `KWayMerger` over the arm's k **already-open, shared**
//! `Arc<SSTableReader>`s (`KWayMerger::new_from_readers`) and drains it to
//! `MergeStep::Complete`. The §3 term being tightened is a *whole-scan* derate, so
//! the timed region deliberately includes producer/adapter setup, `BinaryHeap`
//! refill, cluster assembly and `MergeEntry` construction — all of which scale with
//! k — not only the `ReconcileState` pipeline. Isolating the private reconcile call
//! would both understate the multiplier and add a `pub` item to the hot reconcile
//! path for benchmarking's sake; this bench adds **no** new public surface.
//!
//! Reader OPEN is hoisted out of the timed region (opened once per arm, `Arc`-cloned
//! per iteration — exactly the warm-handle shape `new_from_readers` exists for), so
//! the curve reflects merge cost, not repeated file parsing.
//!
//! ## `now` is pinned through the API
//!
//! TTL-bearing arms pin reconcile-time `now` with `KWayMerger::with_now_secs(Some(
//! PINNED_NOW_SECS))`. The read-path TTL-`now` override env seam
//! (`reader/parsing/row_decoder/now_clock.rs:61`) is `#[cfg(debug_assertions)]` and
//! compiles OUT of the release profile `cargo bench`
//! uses, silently falling back to the wall clock — so it is never used here (by
//! contract this source does not even name that variable). The
//! fixture makes that fallback DETECTABLE: each `ttl_expiring` row carries one cell
//! expired at the pin and one that is live at the pin but expired at any present-day
//! wall clock, and the bench asserts exactly ONE expiry per row.
//!
//! ## Measurements taken on a loaded machine are void
//!
//! The 1-minute load average is read and printed at run start and the run
//! **fails closed** above [`LOAD_CEILING`]; set `CQLITE_BENCH_ALLOW_LOAD=1` to
//! proceed anyway (for a smoke/`--test` run whose numbers are discarded).
//!
//! ## Running
//!
//! ```text
//! cargo bench -p cqlite-core --features write-support --bench reconcile_overlap
//! # smoke (one iteration per arm, numbers not measurements):
//! CQLITE_BENCH_ALLOW_LOAD=1 cargo bench -p cqlite-core --features write-support \
//!   --bench reconcile_overlap -- --test
//! ```
//!
//! Needs no vendored corpus: every generation is synthesized by the write engine,
//! because controlled k is the independent variable and the fetched corpus is
//! single-generation. Registered **advisory-only** in `benches/perf-gate.json` (a
//! measurement instrument must never block a merge).

use criterion::{criterion_group, criterion_main, Criterion};

#[path = "fixtures/mod.rs"]
mod fixtures;

/// 1-minute load-average ceiling for a VALID measurement run. The reference box is
/// 16 cores, so 2.0 is ~12% busy — enough headroom for an editor/ssh session, far
/// below anything that perturbs a single-threaded merge drain.
#[cfg(feature = "write-support")]
const LOAD_CEILING: f64 = 2.0;

#[cfg(feature = "write-support")]
mod overlap {
    use std::sync::Arc;
    use std::time::Duration;

    use criterion::{black_box, BenchmarkId, Criterion, Throughput};

    use cqlite_core::platform::Platform;
    use cqlite_core::schema::TableSchema;
    use cqlite_core::storage::scan_cancel::ScanCancel;
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeStep, RowData};
    use cqlite_core::types::Value;
    use cqlite_core::Config;

    use super::fixtures::multigen::{
        build_multigen, MultigenFixture, OverlapMix, K_VALUES, PINNED_NOW_SECS,
    };
    use super::LOAD_CEILING;

    /// Observables of one full merge drain, all read off the PUBLIC `MergeStep`
    /// stream (no private seam).
    #[derive(Debug, Default, PartialEq, Eq)]
    pub(super) struct DrainStats {
        /// Reconciled output rows (the bench denominator).
        pub output_rows: u64,
        /// Reconciled output partitions.
        pub output_partitions: u64,
        /// Live (non-tombstone) cells across every output row.
        pub live_cells: u64,
        /// Cells whose reconciled value is a tombstone — real cell tombstones plus
        /// expiring cells converted by `expire_ttl_cells` at the pinned `now`.
        pub tombstone_cells: u64,
        /// Output entries that are whole-row tombstones.
        pub row_tombstones: u64,
    }

    /// Read `/proc/loadavg`'s 1-minute figure. `None` when unavailable (non-Linux
    /// or unreadable) — treated as "unknown", which fails closed like an
    /// over-ceiling load.
    fn load_avg_1m() -> Option<f64> {
        let raw = std::fs::read_to_string("/proc/loadavg").ok()?;
        raw.split_whitespace().next()?.parse::<f64>().ok()
    }

    /// Print the run header (machine spec + load) and fail closed when the machine
    /// is not quiesced: numbers taken under load are void, so they must not be
    /// produced at all. `CQLITE_BENCH_ALLOW_LOAD=1` opts out visibly.
    fn assert_quiesced() {
        let load = load_avg_1m();
        let cores = std::thread::available_parallelism().map_or(0, |n| n.get());
        let allow = std::env::var("CQLITE_BENCH_ALLOW_LOAD").as_deref() == Ok("1");
        let load_str = match load {
            Some(l) => format!("{l:.2}"),
            None => "unavailable".to_string(),
        };
        println!(
            "reconcile_overlap: cores={cores} load1m={load_str} ceiling={LOAD_CEILING:.2} \
             allow_load={allow}"
        );
        if allow {
            println!(
                "reconcile_overlap: CQLITE_BENCH_ALLOW_LOAD=1 — results are NOT a valid \
                 measurement and must not be substituted into the derate"
            );
            return;
        }
        match load {
            Some(l) if l <= LOAD_CEILING => {}
            _ => panic!(
                "reconcile_overlap: 1-minute load average {load_str} exceeds the {LOAD_CEILING:.2} \
                 ceiling (or is unavailable) — a measurement taken here is void. Quiesce the \
                 machine and re-run, or set CQLITE_BENCH_ALLOW_LOAD=1 to smoke-run with \
                 discarded numbers."
            ),
        }
    }

    /// Open the fixture's k generations as shared readers, NEWEST-first (run index
    /// 0 = newest), the order `new_from_readers` requires.
    fn open_readers(rt: &tokio::runtime::Runtime, fx: &MultigenFixture) -> Vec<Arc<SSTableReader>> {
        rt.block_on(async {
            let config = Config::default();
            let platform = Arc::new(
                Platform::new(&config)
                    .await
                    .expect("build platform for overlap readers"),
            );
            let mut readers = Vec::with_capacity(fx.data_paths.len());
            for path in &fx.data_paths {
                readers.push(Arc::new(
                    SSTableReader::open(path, &config, platform.clone())
                        .await
                        .expect("open overlap fixture generation"),
                ));
            }
            readers
        })
    }

    /// Build a merger over `readers` at `now_secs` and drain it to completion,
    /// accumulating [`DrainStats`]. Rows are counted, never collected, so a k=20
    /// arm never materializes the merged output.
    fn drain(
        readers: &[Arc<SSTableReader>],
        schema: &TableSchema,
        now_secs: Option<i64>,
    ) -> DrainStats {
        let mut merger =
            KWayMerger::new_from_readers(readers.to_vec(), schema, ScanCancel::new(), None)
                .expect("build KWayMerger over overlap fixture readers")
                // Issue #2043 / design D2: pin `now` through the API. NEVER via
                // the debug-only read-path TTL-`now` override env seam, which
                // compiles out of the release profile `cargo bench` uses.
                .with_now_secs(now_secs);

        let mut stats = DrainStats::default();
        while let MergeStep::Partition { key: _, rows } =
            merger.step().expect("overlap merge step must not error")
        {
            stats.output_partitions += 1;
            for row in &rows {
                stats.output_rows += 1;
                match &row.row_data {
                    RowData::Live { cells } => {
                        for cell in cells {
                            if matches!(cell.value, Value::Tombstone(_)) {
                                stats.tombstone_cells += 1;
                            } else {
                                stats.live_cells += 1;
                            }
                        }
                    }
                    RowData::Tombstone { .. } => stats.row_tombstones += 1,
                }
            }
        }
        stats
    }

    /// Total rows the merge SEES on input, observed through the same public
    /// surface: the sum of each generation's own single-run drain. Authoritative
    /// (it is what the readers emit), unlike the fixture's mutation count, which a
    /// same-generation memtable merge can collapse.
    fn observed_input_rows(
        readers: &[Arc<SSTableReader>],
        schema: &TableSchema,
        now_secs: Option<i64>,
    ) -> u64 {
        readers
            .iter()
            .map(|r| drain(std::slice::from_ref(r), schema, now_secs).output_rows)
            .sum()
    }

    /// The full k × mix matrix.
    pub(super) fn bench_matrix(c: &mut Criterion) {
        assert_quiesced();

        let rt = tokio::runtime::Runtime::new().expect("tokio runtime for overlap readers");
        let mut group = c.benchmark_group("reconcile_overlap");
        group
            .sample_size(20)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(5));

        for mix in OverlapMix::ALL {
            for k in K_VALUES {
                let fx = build_multigen(k, mix);
                let readers = open_readers(&rt, &fx);
                assert_eq!(
                    readers.len(),
                    k,
                    "{}/k{k}: merge input must report exactly k readers",
                    mix.id()
                );
                let now_secs = mix.now_secs();

                // Untimed probe: the arm's authoritative row counts + collision
                // density, and the per-arm invariants that make the timed numbers
                // trustworthy.
                let probe = drain(&readers, &fx.schema, now_secs);
                assert!(
                    probe.output_rows > 0,
                    "{}/k{k}: merge produced 0 rows — the arm measured nothing",
                    mix.id()
                );
                let input_rows = observed_input_rows(&readers, &fx.schema, now_secs);
                assert!(
                    input_rows >= probe.output_rows,
                    "{}/k{k}: observed input rows {input_rows} < output rows {} — \
                     impossible for a merge",
                    mix.id(),
                    probe.output_rows
                );
                if mix == OverlapMix::TtlExpiring {
                    // Exactly ONE expiring cell per row is expired at the pinned
                    // `now` (the other expires only under a wall clock). A silent
                    // wall-clock fallback would double this count, so this assert
                    // is the durable proof that the pin holds in release.
                    assert_eq!(
                        probe.tombstone_cells,
                        probe.output_rows,
                        "{}/k{k}: expected exactly 1 pinned-now TTL expiry per row \
                         (now={PINNED_NOW_SECS}); got {} over {} rows — `now` is not pinned",
                        mix.id(),
                        probe.tombstone_cells,
                        probe.output_rows
                    );
                }

                // Collisions-per-row alongside ns/row (design D1), so cost growth
                // is attributable to real collision density rather than to input
                // size alone. Purge counts are ZERO BY CONSTRUCTION on this read
                // merge — `gc_before_secs` is None and `purge_safe` is false, so
                // the gc-grace purge stage is a strict no-op; the observable
                // deletion work is the tombstone/expiry counts below.
                println!(
                    "reconcile_overlap/{}/k{k}: input_rows={input_rows} output_rows={} \
                     collisions_per_row={:.3} output_partitions={} live_cells={} \
                     tombstone_cells={} row_tombstones={} purges=0(read-merge) \
                     now_secs={:?} fixture_mutations={}",
                    mix.id(),
                    probe.output_rows,
                    input_rows as f64 / probe.output_rows as f64,
                    probe.output_partitions,
                    probe.live_cells,
                    probe.tombstone_cells,
                    probe.row_tombstones,
                    now_secs,
                    fx.input_rows,
                );

                group.throughput(Throughput::Elements(probe.output_rows));
                let schema = fx.schema.clone();
                group.bench_function(BenchmarkId::new(mix.id(), format!("k{k}")), |b| {
                    b.iter(|| {
                        let stats = drain(&readers, &schema, now_secs);
                        // Every timed iteration must do the SAME real work as the
                        // probe — a 0-row or drifting drain can never pass as a
                        // measurement.
                        assert_eq!(stats, probe, "{}/k{k}: drain diverged", mix.id());
                        black_box(stats.output_rows)
                    });
                });

                // Readers (and the fixture temp dir behind them) drop here, before
                // the next arm builds its own — bounded disk/memory across the run.
                drop(readers);
                drop(fx);
            }
        }
        group.finish();
    }
}

/// With `write-support` off there is no `KWayMerger` to measure, so the target
/// compiles to a registered no-op group (mirrors `benches/compaction.rs`).
#[cfg(not(feature = "write-support"))]
fn bench_reconcile_overlap(_c: &mut Criterion) {
    println!("reconcile_overlap: skipped — requires --features write-support");
}

#[cfg(feature = "write-support")]
fn bench_reconcile_overlap(c: &mut Criterion) {
    overlap::bench_matrix(c);
}

criterion_group!(benches, bench_reconcile_overlap);
criterion_main!(benches);
