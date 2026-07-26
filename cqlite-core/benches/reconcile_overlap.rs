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
//! (see [`multigen::OverlapMix`]), plus a two-arm producer-count control. The
//! derived multiplier is `cost(k) / cost(1)` per mix; the record is
//! `docs/research/issue-2043-reconcile-overlap-multiplier.md`.
//!
//! ## What is timed, and why per-drain SETUP must be AMORTIZED, not merely included
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
//! `new_from_readers` spawns one OS producer thread + one adapter open PER
//! GENERATION, so that setup is a **fixed per-scan cost that grows with k**. A real
//! compaction scan pays it once over millions of rows and amortizes it to nothing;
//! an arm whose denominator is a thousand rows does not, and the k = 20 arm would
//! then carry 20 spawns' worth of fixed cost inside a ratio that §3 consumes as a
//! *per-row* multiplier — biasing the multiplier UPWARD with k (roborev + owner
//! decision, 2026-07-26, issue #2043). Two things follow, and both are enforced
//! here rather than caveated in the record:
//!
//! 1. The fixture is sized ([`multigen::OVERLAP_CK`]) so per-row work DOMINATES that
//!    setup at k = 20.
//! 2. The setup is **measured per arm**, not assumed small:
//!    [`overlap::SetupCensus`] times `new_from_readers` alone, the
//!    construct-then-teardown pair, and the whole drain, and every arm prints its
//!    `setup_share_pct`. The record's headline multiplier is the **setup-corrected**
//!    one (each arm's measured setup subtracted), with the raw figure alongside; the
//!    two agree to within the printed share by construction.
//!
//! Reader OPEN is hoisted out of the timed region (opened once per arm, `Arc`-cloned
//! per iteration — exactly the warm-handle shape `new_from_readers` exists for), so
//! the curve reflects merge cost, not repeated file parsing. Construction itself is
//! deliberately NOT hoisted out of the timed region: each producer thread starts
//! filling its bounded channel (`STREAMING_CHANNEL_CAPACITY` = 256 rows) the moment
//! it is spawned, so timing only the post-construction drain would hand every
//! generation a pre-buffered head start and understate cost — a worse distortion
//! than the one being removed. Amortize-and-measure is the honest form.
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
//! ## Measurements taken on a loaded machine are void — checked PER INTERVAL
//!
//! Validity is enforced by [`validity_guard`], which owns the whole gate and fails
//! CLOSED on an unreadable probe as well as on an over-ceiling one. In outline:
//! run-start `loadavg` must be under a host-derived ceiling; then the **foreign**
//! (not-this-process) CPU is sampled once per Criterion sample BATCH and gated on
//! the per-interval **maximum** (a per-arm mean would hide a short burst inside a
//! ≥6 s window), with the mean kept as reported context. Every arm must pass
//! `CpuWatch::end_arm`, and [`overlap::bench_matrix`] asserts that the number of
//! GATED arms equals the number of arms run — so a skipped or unreadable sample can
//! never leave an ungated Criterion number in the record. `SamplingMode::Flat` is
//! pinned so that every PUBLISHED sample is a uniformly long window the foreign-CPU
//! figure can actually resolve at `USER_HZ` granularity (see the call site).
//!
//! `CQLITE_BENCH_ALLOW_LOAD=1` opts out of every tier, visibly, for a
//! smoke/`--test` run whose numbers are discarded.
//!
//! ## The producer-count control arms
//!
//! `producer_control/p1` and `producer_control/p2` are a two-point control that
//! holds the row count, cell count and collision count FIXED (2× the matrix width
//! in disjoint singleton clusters, `o = 1`) and changes only the number of
//! producer/adapter streams the drain fans in: p1 is ONE double-width generation,
//! p2 is TWO standard-width generations. They exist because the `disjoint/k1`
//! anchor's excess over the saturated control is a producer-count effect, and a
//! claim about a mechanism belongs in the instrument as a measured arm rather than
//! in the record as an off-matrix aside.
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

#[path = "profiling/mod.rs"]
mod profiling;

// Included HERE rather than declared inside `fixtures/mod.rs`: that module is
// `#[path]`-included by every bench target, so declaring the multigen fixtures
// there would make ~10 targets compile them for the benefit of this one. The
// validity guard is included the same way and for the same reason.
#[cfg(feature = "write-support")]
#[path = "fixtures/multigen.rs"]
mod multigen;

#[cfg(feature = "write-support")]
#[path = "fixtures/validity_guard.rs"]
mod validity_guard;

#[cfg(feature = "write-support")]
mod overlap {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use criterion::{black_box, BenchmarkId, Criterion, SamplingMode, Throughput};

    use cqlite_core::platform::Platform;
    use cqlite_core::schema::TableSchema;
    use cqlite_core::storage::scan_cancel::ScanCancel;
    use cqlite_core::storage::sstable::reader::SSTableReader;
    use cqlite_core::storage::write_engine::merge::{KWayMerger, MergeStep, RowData};
    use cqlite_core::types::Value;
    use cqlite_core::Config;

    use super::multigen::{
        build_multigen_sized, MultigenFixture, OverlapMix, CLUSTERS_PER_GEN, K_VALUES, OVERLAP_CK,
        PINNED_NOW_SECS, PRODUCER_CONTROL_CK,
    };
    use super::validity_guard::{self, CpuWatch};

    /// Materialized cells a FULLY-LIVE reconciled row of the fixture table
    /// carries: the clustering column `ck` plus the three value columns `v0`,
    /// `v1`, `v2`. `ck` is materialized as a cell by the read path (observed, not
    /// assumed — the shape assertions below fail loudly if it ever stops being),
    /// which is why the record quotes 4 cells/row for this fixture.
    const CELLS_PER_LIVE_ROW: u64 = 4;

    /// Repetitions the per-arm [`SetupCensus`] takes a median over. Odd, and small
    /// enough that the census costs a fraction of the arm's own ≥5 s measurement
    /// window while still rejecting a single outlier drain.
    const SETUP_CENSUS_REPS: usize = 5;

    /// Observables of one full merge drain, all read off the PUBLIC `MergeStep`
    /// stream (no private seam).
    #[derive(Debug, Default, Clone, PartialEq, Eq)]
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
        /// Output entries that are LIVE rows carrying a coexisting row deletion
        /// (`MergeEntry::row_deletion`, issue #932) — a row tombstone older than
        /// the surviving cells. This is the observable that proves the `tombstone`
        /// mix really presents live-cells-vs-row-tombstone to the merge instead of
        /// a cell-less row tombstone.
        pub coexisting_row_deletions: u64,
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

    /// The per-drain SETUP the timed region contains: k producer-thread spawns + k
    /// adapter opens, on the consumer thread.
    fn build_merger(
        readers: &[Arc<SSTableReader>],
        schema: &TableSchema,
        now_secs: Option<i64>,
    ) -> KWayMerger {
        KWayMerger::new_from_readers(readers.to_vec(), schema, ScanCancel::new(), None)
            .expect("build KWayMerger over overlap fixture readers")
            // Issue #2043 / design D2: pin `now` through the API. NEVER via
            // the debug-only read-path TTL-`now` override env seam, which
            // compiles out of the release profile `cargo bench` uses.
            .with_now_secs(now_secs)
    }

    /// Drain `merger` to completion, accumulating [`DrainStats`]. Rows are counted,
    /// never collected, so a k=20 arm never materializes the merged output.
    fn drain_merger(merger: &mut KWayMerger) -> DrainStats {
        let mut stats = DrainStats::default();
        while let MergeStep::Partition { key: _, rows } =
            merger.step().expect("overlap merge step must not error")
        {
            stats.output_partitions += 1;
            for row in &rows {
                stats.output_rows += 1;
                match &row.row_data {
                    RowData::Live { cells } => {
                        // Issue #932: a row deletion OLDER than the surviving cells
                        // rides on the entry beside `RowData::Live`. Counting it is
                        // what distinguishes "live cells collided with a row
                        // tombstone" from "the row tombstone ate the cells".
                        if row.row_deletion.is_some() {
                            stats.coexisting_row_deletions += 1;
                        }
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

    /// Build a merger over `readers` at `now_secs` and drain it — exactly the work
    /// one timed iteration does.
    fn drain(
        readers: &[Arc<SSTableReader>],
        schema: &TableSchema,
        now_secs: Option<i64>,
    ) -> DrainStats {
        let mut merger = build_merger(readers, schema, now_secs);
        drain_merger(&mut merger)
    }

    /// Per-GENERATION single-run drain stats, ordered by generation index (oldest
    /// = generation 0 first — the reverse of the readers' newest-first order).
    ///
    /// Two consumers: the sum of `output_rows` is the merge's authoritative INPUT
    /// row count (what the readers really emit, unlike the fixture's mutation
    /// count, which the flush collapses), and the vector itself is the composition
    /// census that proves each generation's contribution is k-invariant.
    fn per_generation_stats(
        readers: &[Arc<SSTableReader>],
        schema: &TableSchema,
        now_secs: Option<i64>,
    ) -> Vec<DrainStats> {
        let mut per_gen: Vec<DrainStats> = readers
            .iter()
            .map(|r| drain(std::slice::from_ref(r), schema, now_secs))
            .collect();
        per_gen.reverse();
        per_gen
    }

    /// MEASURED per-drain setup cost for one arm — the instrument's own answer to
    /// "how much of this arm's number is fixed per-scan cost?" (owner decision
    /// 2026-07-26, issue #2043).
    ///
    /// All three figures are medians over [`SETUP_CENSUS_REPS`] untimed reps of the
    /// SAME work a timed iteration does:
    ///
    /// * `construct` — `new_from_readers` alone: the serial, per-scan, k-scaling
    ///   setup (k thread spawns + k adapter opens) that sits INSIDE the timed
    ///   region and that a million-row field scan amortizes away. This is the
    ///   numerator of `setup_share`, and the quantity the record's
    ///   setup-corrected headline subtracts.
    /// * `construct_teardown` — construct then immediately drop (spawn + cancel +
    ///   join, no rows drained): an upper bound on the fixed cost, reported so the
    ///   correction cannot be accused of picking the flattering half.
    /// * `total` — construct + full drain: the whole timed-iteration cost, so
    ///   `share = construct / total` is computed from two figures taken by the same
    ///   clock in the same loop (and `total` doubles as a cross-check on Criterion's
    ///   median for the arm).
    #[derive(Debug, Clone, Copy)]
    pub(super) struct SetupCensus {
        construct: Duration,
        construct_teardown: Duration,
        total: Duration,
    }

    impl SetupCensus {
        /// Fraction of one timed iteration that is per-scan SETUP.
        fn share(&self) -> f64 {
            if self.total.is_zero() {
                0.0
            } else {
                self.construct.as_secs_f64() / self.total.as_secs_f64()
            }
        }
    }

    fn median(mut samples: Vec<Duration>) -> Duration {
        samples.sort_unstable();
        samples[samples.len() / 2]
    }

    fn measure_setup(
        readers: &[Arc<SSTableReader>],
        schema: &TableSchema,
        now_secs: Option<i64>,
        arm_id: &str,
        expect: &DrainStats,
    ) -> SetupCensus {
        let mut construct = Vec::with_capacity(SETUP_CENSUS_REPS);
        let mut total = Vec::with_capacity(SETUP_CENSUS_REPS);
        for _ in 0..SETUP_CENSUS_REPS {
            let started = Instant::now();
            let mut merger = build_merger(readers, schema, now_secs);
            construct.push(started.elapsed());
            let stats = drain_merger(&mut merger);
            total.push(started.elapsed());
            // The census must do the same real work as the timed region, or the
            // share it reports describes a different drain.
            assert_eq!(
                &stats, expect,
                "{arm_id}: setup-census drain diverged from the arm's verified shape"
            );
        }
        let mut teardown = Vec::with_capacity(SETUP_CENSUS_REPS);
        for _ in 0..SETUP_CENSUS_REPS {
            let started = Instant::now();
            drop(build_merger(readers, schema, now_secs));
            teardown.push(started.elapsed());
        }
        SetupCensus {
            construct: median(construct),
            construct_teardown: median(teardown),
            total: median(total),
        }
    }

    /// The census an arm MUST produce if the collision shape it documents is
    /// really present after the flush.
    ///
    /// This exists because a fixture can degenerate silently: a row tombstone
    /// stamped above its generation's live cells is reconciled away AT FLUSH TIME,
    /// leaving a cell-less row tombstone, so the arm would measure
    /// tombstone-vs-tombstone while still printing plausible ns/row (roborev,
    /// issue #2043). Every mix is covered, and the expectation is derived from the
    /// fixture's construction rather than copied from a previous run's output.
    #[derive(Debug)]
    struct ExpectedShape {
        input_rows: u64,
        drain: DrainStats,
    }

    fn expected_shape(mix: OverlapMix, k: usize, ck_per_gen: usize) -> ExpectedShape {
        use super::multigen::OVERLAP_PARTITIONS;

        let partitions = OVERLAP_PARTITIONS as u64;
        let clusters = partitions * ck_per_gen as u64;
        let (half, quarter) = (clusters / 2, clusters / 4);
        let k64 = k as u64;
        let k_odd = k % 2 == 1;
        let base = DrainStats {
            output_partitions: partitions,
            ..DrainStats::default()
        };
        // Cells of a row with `t` of its value columns reconciling to a tombstone.
        let live = |rows: u64, tombstoned: u64| rows * (CELLS_PER_LIVE_ROW - tombstoned);
        match mix {
            // Every generation contributes its own disjoint ck window: no
            // collisions at all, so output == input and every row is fully live.
            OverlapMix::Disjoint => ExpectedShape {
                input_rows: k64 * clusters,
                drain: DrainStats {
                    output_rows: k64 * clusters,
                    live_cells: live(k64 * clusters, 0),
                    ..base
                },
            },
            // k copies of every cluster collapse to one fully-live row.
            OverlapMix::LwwOverwrite => ExpectedShape {
                input_rows: k64 * clusters,
                drain: DrainStats {
                    output_rows: clusters,
                    live_cells: live(clusters, 0),
                    ..base
                },
            },
            // Lower half: a FULLY LIVE row PLUS a coexisting row deletion (the row
            // tombstone sits below the cells, so both survive — the shape this arm
            // exists to measure). Upper half: `v1` is a surviving cell tombstone.
            // NO output row may be a whole-row tombstone.
            OverlapMix::Tombstone => ExpectedShape {
                input_rows: k64 * clusters,
                drain: DrainStats {
                    output_rows: clusters,
                    live_cells: live(half, 0) + live(half, 1),
                    tombstone_cells: half,
                    coexisting_row_deletions: half,
                    ..base
                },
            },
            // One of the two expiring cells (`v0`) is expired at the pinned `now`.
            OverlapMix::TtlExpiring => ExpectedShape {
                input_rows: k64 * clusters,
                drain: DrainStats {
                    output_rows: clusters,
                    live_cells: live(clusters, 1),
                    tombstone_cells: clusters,
                    ..base
                },
            },
            // By slot quarter: singleton (gen 0 only) / per-column blend / live +
            // alternating tombstone kind / expiring. Generation 0 writes all four
            // quarters, later generations three (the singleton quarter is written
            // once), which is why `o` < k for this mix.
            OverlapMix::FieldBlend => {
                // Blend quarter: at k=1 only the even-generation column set (`v0`,
                // `v2`) exists, so `v1` is absent; from k=2 the merge unions the
                // per-column winners of an even and an odd generation.
                let blend_live = if k == 1 {
                    live(quarter, 1)
                } else {
                    live(quarter, 0)
                };
                // Tombstone quarter: newest generation EVEN ⇒ its `v0` cell
                // tombstone is the surviving `v0`; newest generation ODD ⇒ `v0` is
                // live again and only the (older) row deletion coexists.
                let (tomb_live, tomb_cells) = if k_odd {
                    (live(quarter, 1), quarter)
                } else {
                    (live(quarter, 0), 0)
                };
                // A row deletion (written by every ODD generation) survives beside
                // the newer cells as soon as one odd generation exists.
                let coexisting = if k >= 2 { quarter } else { 0 };
                ExpectedShape {
                    input_rows: 4 * quarter + (k64 - 1) * 3 * quarter,
                    drain: DrainStats {
                        output_rows: 4 * quarter,
                        // singleton + blend + tombstone + expiring quarters.
                        live_cells: live(quarter, 0) + blend_live + tomb_live + live(quarter, 1),
                        tombstone_cells: tomb_cells + quarter,
                        coexisting_row_deletions: coexisting,
                        ..base
                    },
                }
            }
        }
    }

    /// Build, verify and time ONE arm. Returns the arm's probe census so a caller
    /// can compare arms (the producer-count control asserts its two arms carry an
    /// identical row/cell population).
    fn run_arm(
        group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
        rt: &tokio::runtime::Runtime,
        watch: &mut CpuWatch,
        census: &mut std::collections::HashMap<String, Vec<DrainStats>>,
        arm: ArmSpec,
    ) -> DrainStats {
        let ArmSpec {
            mix,
            k,
            ck_per_gen,
            id_group,
            id_param,
        } = arm;
        let arm_id = format!("{id_group}/{id_param}");
        // Re-sampled per arm: a 27-arm run spans minutes, so a single up-front
        // sample certifies nothing about the arm running now.
        let load = watch.sample_load();

        let fx = build_multigen_sized(k, mix, ck_per_gen);
        let readers = open_readers(rt, &fx);
        assert_eq!(
            readers.len(),
            k,
            "{arm_id}: merge input must report exactly k={k} readers"
        );
        assert_eq!(
            (fx.k, fx.mix, fx.ck_per_gen),
            (k, mix, ck_per_gen),
            "{arm_id}: the fixture built a different arm than this one requested"
        );
        let now_secs = mix.now_secs();

        // Untimed probe: the arm's authoritative row counts + collision density,
        // and the invariants that make the timed numbers trustworthy.
        let probe = drain(&readers, &fx.schema, now_secs);
        let per_gen = per_generation_stats(&readers, &fx.schema, now_secs);
        let input_rows: u64 = per_gen.iter().map(|s| s.output_rows).sum();
        assert!(
            input_rows >= probe.output_rows && probe.output_rows > 0,
            "{arm_id}: observed input rows {input_rows} vs output rows {} — impossible for a merge",
            probe.output_rows
        );

        // The arm's DOCUMENTED collision shape, asserted as a full census. A
        // silently-degenerate fixture can therefore never emit numbers.
        let expected = expected_shape(mix, k, ck_per_gen);
        assert_eq!(
            probe, expected.drain,
            "{arm_id}: reconciled shape does not match the documented collision mix \
             (left = measured, right = expected)"
        );
        assert_eq!(
            input_rows, expected.input_rows,
            "{arm_id}: observed merge input rows do not match the fixture's construction"
        );
        if mix == OverlapMix::TtlExpiring {
            // Exactly ONE expiring cell per row is expired at the pinned `now` (the
            // other expires only under a wall clock). A silent wall-clock fallback
            // would double this count, so this assert is the durable proof that the
            // pin holds in release.
            assert_eq!(
                probe.tombstone_cells, probe.output_rows,
                "{arm_id}: expected exactly 1 pinned-now TTL expiry per row \
                 (now={PINNED_NOW_SECS}); got {} over {} rows — `now` is not pinned",
                probe.tombstone_cells, probe.output_rows
            );
        }

        // k-INVARIANCE: generation g must contribute the same census at every k, or
        // `cost(k)/cost(1)` would conflate cluster depth with composition (roborev,
        // issue #2043 — the former `(generation + k) % 2` tombstone alternation).
        let key = format!("{id_group}/ck{ck_per_gen}");
        let known = census.entry(key).or_default();
        for (gen, stats) in per_gen.iter().enumerate() {
            match known.get(gen) {
                Some(prev) => assert_eq!(
                    stats, prev,
                    "{arm_id}: generation {gen}'s census differs from the same generation in a \
                     previous k arm — the fixture's composition is k-DEPENDENT"
                ),
                None => known.push(stats.clone()),
            }
        }

        // Collisions-per-row alongside ns/row (design D1), so cost growth is
        // attributable to real collision density rather than to input size alone.
        // Purge counts are ZERO BY CONSTRUCTION on this read merge — `gc_before_secs`
        // is None and `purge_safe` is false, so the gc-grace purge stage is a strict
        // no-op; the observable deletion work is the tombstone/expiry counts below.
        println!(
            "reconcile_overlap/{arm_id}: mix={} k={k} ck_per_gen={ck_per_gen} \
             load1m={} input_rows={input_rows} output_rows={} collisions_per_row={:.3} \
             output_partitions={} live_cells={} tombstone_cells={} row_tombstones={} \
             coexisting_row_deletions={} purges=0(read-merge) now_secs={now_secs:?} \
             fixture_mutations={}",
            mix.id(),
            load.map_or_else(|| "unavailable".to_string(), |l| format!("{l:.2}")),
            probe.output_rows,
            input_rows as f64 / probe.output_rows as f64,
            probe.output_partitions,
            probe.live_cells,
            probe.tombstone_cells,
            probe.row_tombstones,
            probe.coexisting_row_deletions,
            fx.mutations_written,
        );

        // MEASURED per-scan setup share (owner decision 2026-07-26): the record's
        // headline multiplier subtracts `setup_construct_ns`, so it must be a
        // measured per-arm figure, printed next to the arm it corrects.
        let setup = measure_setup(&readers, &fx.schema, now_secs, &arm_id, &probe);
        println!(
            "reconcile_overlap/{arm_id}: setup_construct_ns={} setup_construct_teardown_ns={} \
             drain_total_ns={} setup_share_pct={:.2} setup_construct_ns_per_output_row={:.1} \
             census_reps={SETUP_CENSUS_REPS}",
            setup.construct.as_nanos(),
            setup.construct_teardown.as_nanos(),
            setup.total.as_nanos(),
            setup.share() * 100.0,
            setup.construct.as_nanos() as f64 / probe.output_rows as f64,
        );

        group.throughput(Throughput::Elements(probe.output_rows));
        let schema = fx.schema.clone();
        let timed = probe.clone();
        // Foreign-CPU windows are opened and closed around EACH Criterion sample
        // batch (`iter_custom`), not once around the whole `bench_function`: the
        // gate is the per-interval maximum, so a short foreign burst cannot be
        // averaged away over a ≥6 s arm (roborev, issue #2043). The returned
        // duration is the same quantity `b.iter` would have measured — the batch's
        // wall time over `iters` identical drains.
        let cpu = &mut *watch;
        cpu.begin_arm();
        group.bench_function(BenchmarkId::new(id_group, id_param), |b| {
            b.iter_custom(|iters| {
                let before = cpu.interval_start();
                let started = Instant::now();
                for _ in 0..iters {
                    let stats = drain(&readers, &schema, now_secs);
                    // Every timed iteration must do the SAME real work as the
                    // probe — a 0-row or drifting drain can never pass as a
                    // measurement.
                    assert_eq!(stats, timed, "{arm_id}: drain diverged");
                    black_box(stats.output_rows);
                }
                let elapsed = started.elapsed();
                cpu.observe_interval(&arm_id, before, elapsed);
                elapsed
            });
        });
        let foreign = cpu.end_arm(&arm_id);
        println!(
            "reconcile_overlap/{arm_id}: foreign_cpu_cores max={:.3} mean={:.3} (ceiling {:.2}) \
             own_cpu_cores={:.2} gated_intervals={}/{} gated_wall={:.1}%",
            foreign.max_cores,
            foreign.mean_cores,
            cpu.foreign_ceiling(),
            foreign.own_cores,
            foreign.gated_intervals,
            foreign.total_intervals,
            foreign.gated_wall_fraction * 100.0,
        );

        // Readers (and the fixture temp dir behind them) drop here, before the next
        // arm builds its own — bounded disk/memory across the run.
        drop(readers);
        drop(fx);
        probe
    }

    /// One arm of the run: which fixture to build and how to name it.
    struct ArmSpec {
        mix: OverlapMix,
        k: usize,
        ck_per_gen: usize,
        id_group: &'static str,
        id_param: String,
    }

    /// Criterion id group of the producer-count control pair.
    const PRODUCER_CONTROL_GROUP: &str = "producer_control";

    /// Every arm this target runs, in run order: the full k × mix matrix followed by
    /// the producer-count control pair. Built as data so the arm IDS have exactly ONE
    /// definition, shared by the measuring path and `--list` enumeration.
    fn arm_specs() -> Vec<ArmSpec> {
        let mut specs: Vec<ArmSpec> = Vec::new();
        for mix in OverlapMix::ALL {
            for k in K_VALUES {
                specs.push(ArmSpec {
                    mix,
                    k,
                    ck_per_gen: OVERLAP_CK,
                    id_group: mix.id(),
                    id_param: format!("k{k}"),
                });
            }
        }
        // Producer-count control: identical rows, cells and collisions (o = 1);
        // ONE producer (p1, a double-width single generation) vs TWO (p2, the
        // standard-width k=2 fixture). The measured mechanism behind the k=1
        // anchor's excess over the saturated control, as a real arm.
        specs.push(ArmSpec {
            mix: OverlapMix::Disjoint,
            k: 1,
            ck_per_gen: PRODUCER_CONTROL_CK,
            id_group: PRODUCER_CONTROL_GROUP,
            id_param: "p1".to_string(),
        });
        specs.push(ArmSpec {
            mix: OverlapMix::Disjoint,
            k: 2,
            ck_per_gen: OVERLAP_CK,
            id_group: PRODUCER_CONTROL_GROUP,
            id_param: "p2".to_string(),
        });
        specs
    }

    /// Criterion's `--list` enumerates benchmark ids and measures NOTHING.
    fn list_only() -> bool {
        std::env::args().any(|a| a == "--list")
    }

    /// The full k × mix matrix, plus the producer-count control pair.
    pub(super) fn bench_matrix(c: &mut Criterion) {
        // Both `/proc` parsers, over captured samples, before anything is measured
        // (see `validity_guard::assert_probe_parsers_sound` for why this is a
        // runtime self-check and not a `#[test]`).
        validity_guard::assert_probe_parsers_sound();

        let specs = arm_specs();
        let arms = specs.len();

        let mut group = c.benchmark_group("reconcile_overlap");
        group
            .sample_size(20)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(5))
            // FLAT sampling, pinned rather than left to `Auto` (issue #2043). Two
            // reasons, both about this instrument specifically:
            //
            // 1. `Auto` picks LINEAR for the cheap arms and FLAT for the expensive
            //    ones (its rule is "would the linear plan exceed 2× the target
            //    time?"), so the matrix would sample its k = 1 and its k = 20 arms
            //    by DIFFERENT schemes — and linear's first samples run `d`, `2d`,
            //    `3d`… iterations, i.e. a few tens of milliseconds. Flat gives every
            //    arm the same scheme and every sample the same iteration count.
            // 2. Those short linear samples are also too short for the validity
            //    guard's foreign-CPU figure to RESOLVE its ceiling at USER_HZ
            //    granularity (`validity_guard::MIN_GATEABLE_TOTAL_TICKS`): a 54 ms
            //    window advances ~86 `/proc/stat` ticks on 16 cores, where ONE stray
            //    tick reads as 0.19 cores. Under Flat every PUBLISHED sample is
            //    `measurement_time / sample_size` (250 ms, ~400 ticks) or longer, so
            //    every published sample is gated on a resolvable window and only
            //    Criterion's discarded warm-up probes are unresolvable.
            //
            // Criterion's own caveat for Flat — that it cannot fit the per-iteration
            // slope — does not apply to what this record publishes: with iterations
            // of 20–240 ms this bench is squarely in the "very long-running" regime
            // Flat exists for, and the reported figure is the per-sample median.
            .sampling_mode(SamplingMode::Flat);

        // `--list` enumeration must be FREE. This target's fixture synthesis, probe
        // drain and per-generation drains run OUTSIDE `bench_function` (they produce
        // the census that makes a timed number trustworthy), so a naive list run
        // would flush all 27 arms' generations and could even fail closed in
        // `assert_quiesced`/`observe_interval` while measuring nothing (roborev,
        // issue #2043). Register the ids from the SAME `arm_specs()` the measuring
        // path uses — criterion never executes a routine in list mode, and this
        // branch is unreachable in any measuring run (`--test` included), so nothing
        // measured changes.
        if list_only() {
            println!(
                "reconcile_overlap: --list — enumerating {arms} arm ids only; no fixture is \
                 built, no validity gate runs and nothing is measured"
            );
            for arm in &specs {
                group.bench_function(BenchmarkId::new(arm.id_group, &arm.id_param), |b| {
                    b.iter(|| ())
                });
            }
            group.finish();
            return;
        }

        let mut watch = CpuWatch::new();
        validity_guard::assert_quiesced(&mut watch);

        let rt = tokio::runtime::Runtime::new().expect("tokio runtime for overlap readers");
        let mut census: std::collections::HashMap<String, Vec<DrainStats>> =
            std::collections::HashMap::new();

        let mut control: Vec<DrainStats> = Vec::new();
        for arm in specs {
            let is_control = arm.id_group == PRODUCER_CONTROL_GROUP;
            let stats = run_arm(&mut group, &rt, &mut watch, &mut census, arm);
            if is_control {
                control.push(stats);
            }
        }

        let [p1, p2] = control.as_slice() else {
            panic!(
                "producer_control: exactly 2 control arms must have run, got {}",
                control.len()
            )
        };
        assert_eq!(
            (p1.output_rows, p1.live_cells, p1.tombstone_cells),
            (p2.output_rows, p2.live_cells, p2.tombstone_cells),
            "producer_control: p1 and p2 must present an IDENTICAL row and cell population — \
             only the producer count may differ"
        );
        assert_eq!(
            p1.output_rows,
            2 * CLUSTERS_PER_GEN as u64,
            "producer_control: both arms must carry 2x the matrix width in rows"
        );

        group.finish();
        // The producer count is `KWayMerger`'s one thread per generation at the
        // matrix's LARGEST k — derived from `K_VALUES`, never a literal, so editing
        // the matrix can never emit a wrong figure into the operator-facing validity
        // summary the record quotes (roborev, issue #2043).
        watch.report(arms, K_VALUES.iter().copied().max().unwrap_or(0));

        // EVERY arm must have been gated. Without this, one arm whose foreign-CPU
        // probe was skipped (unreadable `/proc`, a `hidepid` mount, a transient read
        // error) would print `unavailable` and still have its Criterion number
        // published as if validated — the run would look valid while carrying an
        // ungated measurement (roborev, issue #2043).
        if validity_guard::load_opt_out() {
            println!(
                "reconcile_overlap: arms_gated={}/{arms} not enforced — \
                 CQLITE_BENCH_ALLOW_LOAD=1 (this run is not a measurement)",
                watch.arms_gated()
            );
        } else {
            assert_eq!(
                watch.arms_gated(),
                arms,
                "reconcile_overlap: only {} of {arms} arms passed the per-interval foreign-CPU \
                 gate — a number that was not gated must not be published, so this run is void",
                watch.arms_gated(),
            );
        }
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

// Shared criterion config, as every other gated bench in this crate declares it:
// `profiling::configure()` attaches the pprof sampler so `--profile-time <secs>`
// writes a flamegraph (`benches/profiling/mod.rs`). That matters most HERE — this
// is the target whose purpose is decomposing where per-row k-cost goes. It is
// measurement-NEUTRAL: `configure()` returns `Criterion::default()` plus a
// profiler criterion activates only under `--profile-time`, and this bench's group
// sets `sample_size`/`warm_up_time`/`measurement_time` explicitly regardless. ONE
// group serves both feature states because both `bench_reconcile_overlap` variants
// share the name.
criterion_group!(
    name = benches;
    config = profiling::configure();
    targets = bench_reconcile_overlap
);
criterion_main!(benches);
