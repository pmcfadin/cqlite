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
//! ## Measurements taken on a loaded machine are void — checked PER ARM
//!
//! A 27-arm run spans minutes, so one up-front load sample certifies nothing about
//! the arms that run last. Validity is therefore enforced in two tiers:
//!
//! 1. **Run start:** the 1-minute load average must be ≤ [`load_ceiling`] before
//!    the instrument adds any load of its own — else the run fails closed.
//! 2. **Every arm:** the **foreign** (not-this-process) CPU consumed during that
//!    arm's TIMED region must stay under [`foreign_cpu_ceiling`], computed
//!    from `/proc/stat` minus this process's own `utime+stime` and printed per
//!    arm. Exceeding it voids the arm and fails the run.
//!
//! BOTH ceilings scale with the host's core count (a fixed figure tuned for the
//! 16-core reference box would admit 4× as much relative interference on a 4-core
//! one) and BOTH are printed in the run header, so a record always shows the values
//! actually enforced. On 16 cores they are the `2.00` / `1.00` cores the banked runs
//! enforced.
//!
//! Tier 2 is deliberately NOT a per-arm loadavg check: `KWayMerger` runs one
//! producer thread per generation, so the run's OWN parallelism pushes `loadavg`
//! past any useful ceiling by k = 10 — a per-arm loadavg gate fails on the
//! instrument itself rather than on interference (observed while building this).
//! `loadavg` is still sampled per arm and reported (min/max/spread) so drift is
//! visible in the record, but it is informational, not a gate.
//!
//! `CQLITE_BENCH_ALLOW_LOAD=1` opts out of both tiers, visibly, for a
//! smoke/`--test` run whose numbers are discarded.
//!
//! **Platform restriction:** both probes read `/proc`, which is **Linux-only**. An
//! unreadable probe is treated as unknown and fails closed, so on a non-Linux host
//! this instrument cannot produce a valid measurement at all — only a
//! self-labelled `CQLITE_BENCH_ALLOW_LOAD=1` non-measurement run. The record's
//! numbers are Linux measurements by construction.
//!
//! ## The producer-count control arms
//!
//! `producer_control/p1` and `producer_control/p2` are a two-point control that
//! holds the row count, cell count and collision count FIXED (2048 disjoint
//! singleton clusters, `o = 1`) and changes only the number of producer/adapter
//! streams the drain fans in: p1 is ONE double-width generation, p2 is TWO
//! standard-width generations. They exist because the `disjoint/k1` anchor's
//! excess over the saturated control is a producer-count effect, and a claim
//! about a mechanism belongs in the instrument as a measured arm rather than in
//! the record as an off-matrix aside.
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
// there would make ~10 targets compile them for the benefit of this one.
#[cfg(feature = "write-support")]
#[path = "fixtures/multigen.rs"]
mod multigen;

/// Per-CORE 1-minute load-average budget for a VALID measurement run: the machine
/// must be under ~12.5 % busy before the instrument adds any load of its own —
/// enough headroom for an editor/ssh session, far below anything that perturbs a
/// merge drain.
///
/// Derived from the host's core count rather than fixed (roborev, issue #2043): an
/// absolute `2.0` tuned for the 16-core reference box admits a **50 %-busy**
/// 4-core box, silently weakening the "loaded measurements are void" guarantee by
/// 4×. The enforced figure is [`load_ceiling`] and is printed in the run header, so
/// every record shows what was actually enforced. On the 16-core reference box it is
/// exactly the `2.0` the banked runs enforced, so no banked number shifts.
#[cfg(feature = "write-support")]
const LOAD_CEILING_PER_CORE: f64 = 0.125;

/// Floor for [`load_ceiling`] on very small hosts: at 1–4 cores the per-core budget
/// (0.125–0.5) would sit inside the noise of an otherwise-idle box's own loadavg
/// decay, so the gate would fail on nothing. 0.5 keeps a 4-core host at the same
/// 12.5 % it derives anyway and never goes stricter than that.
#[cfg(feature = "write-support")]
const LOAD_CEILING_FLOOR: f64 = 0.5;

/// Foreign-CPU budget per CORE, in cores: 1/16 core per core is ~6 % of the box —
/// enough for an ssh/editor session, far below anything that perturbs a merge drain.
/// Derived for the same reason as [`LOAD_CEILING_PER_CORE`]: a fixed `1.0` core is
/// 6 % of a 16-core box but **25 %** of a 4-core one. See [`foreign_cpu_ceiling`];
/// on the 16-core reference box it is exactly the `1.00` the banked runs enforced.
#[cfg(feature = "write-support")]
const FOREIGN_CPU_CEILING_PER_CORE: f64 = 0.0625;

/// Floor for [`foreign_cpu_ceiling`], in cores — a tiny host still gets a quarter
/// core of slack, below which sampling jitter alone would void arms.
#[cfg(feature = "write-support")]
const FOREIGN_CPU_CEILING_FLOOR: f64 = 0.25;

/// The enforced 1-minute load ceiling on a host with `cores` cores.
#[cfg(feature = "write-support")]
fn load_ceiling(cores: f64) -> f64 {
    (LOAD_CEILING_PER_CORE * cores).max(LOAD_CEILING_FLOOR)
}

/// The enforced per-arm FOREIGN-CPU ceiling, in whole cores, on a host with `cores`
/// cores. Exceeding it VOIDS the arm.
#[cfg(feature = "write-support")]
fn foreign_cpu_ceiling(cores: f64) -> f64 {
    (FOREIGN_CPU_CEILING_PER_CORE * cores).max(FOREIGN_CPU_CEILING_FLOOR)
}

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

    use super::multigen::{
        build_multigen_sized, MultigenFixture, OverlapMix, CLUSTERS_PER_GEN, K_VALUES, OVERLAP_CK,
        PINNED_NOW_SECS, PRODUCER_CONTROL_CK,
    };
    use super::{foreign_cpu_ceiling, load_ceiling};

    /// Materialized cells a FULLY-LIVE reconciled row of the fixture table
    /// carries: the clustering column `ck` plus the three value columns `v0`,
    /// `v1`, `v2`. `ck` is materialized as a cell by the read path (observed, not
    /// assumed — the shape assertions below fail loudly if it ever stops being),
    /// which is why the record quotes 4 cells/row for this fixture.
    const CELLS_PER_LIVE_ROW: u64 = 4;

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

    /// Read `/proc/loadavg`'s 1-minute figure. **Linux-only**: `None` on any other
    /// platform (or an unreadable proc) — treated as "unknown", which fails closed
    /// exactly like an over-ceiling load, so a non-Linux host can only ever produce
    /// a self-labelled non-measurement run.
    fn load_avg_1m() -> Option<f64> {
        let raw = std::fs::read_to_string("/proc/loadavg").ok()?;
        raw.split_whitespace().next()?.parse::<f64>().ok()
    }

    /// `CQLITE_BENCH_ALLOW_LOAD=1` — the visible opt-out that turns the run into a
    /// self-labelled NON-measurement.
    fn load_opt_out() -> bool {
        std::env::var("CQLITE_BENCH_ALLOW_LOAD").as_deref() == Ok("1")
    }

    /// Cumulative CPU-tick counters for the machine and for THIS process, read
    /// from `/proc/stat` + `/proc/self/stat` (Linux-only, same USER_HZ unit — only
    /// ratios are taken, so the tick length never needs to be known).
    #[derive(Debug, Clone, Copy)]
    struct CpuTicks {
        /// All jiffies across all CPUs (capacity of the box over the interval).
        total: u64,
        /// Idle + iowait jiffies.
        idle: u64,
        /// This process's user+system jiffies, children included.
        own: u64,
    }

    fn cpu_ticks() -> Option<CpuTicks> {
        let stat = std::fs::read_to_string("/proc/stat").ok()?;
        let cpu_line = stat.lines().find(|l| l.starts_with("cpu "))?;
        let fields: Vec<u64> = cpu_line
            .split_whitespace()
            .skip(1)
            .filter_map(|f| f.parse::<u64>().ok())
            .collect();
        // user nice system idle iowait irq softirq steal [guest guest_nice]
        if fields.len() < 5 {
            return None;
        }
        // Sum ONLY user..=steal (the first EIGHT fields, when present). The kernel
        // reports `guest`/`guest_nice` as a SUBSET already counted inside
        // `user`/`nice`, so summing them double-counts every guest jiffy: on a KVM
        // host both `total` and `busy` inflate and `foreign_cores =
        // (busy - own)/total * cores` skews toward OVER-reporting, i.e. spurious
        // voided arms (roborev, issue #2043). The slice is length-clamped, so a
        // short/extended `/proc/stat` line can never panic here.
        let busy_fields = &fields[..fields.len().min(8)];
        let total: u64 = busy_fields.iter().copied().sum();
        let idle = fields[3].saturating_add(fields[4]);
        let me = std::fs::read_to_string("/proc/self/stat").ok()?;
        // Field 2 is `(comm)`, which may contain spaces; split after the last ')'.
        let tail = &me[me.rfind(')')? + 1..];
        let f: Vec<u64> = tail
            .split_whitespace()
            .map(|t| t.parse::<u64>().unwrap_or(0))
            .collect();
        // After `state` (index 0 here): ppid pgrp session tty tpgid flags minflt
        // cminflt majflt cmajflt utime stime cutime cstime → utime at index 11.
        if f.len() < 15 {
            return None;
        }
        let own = f[11]
            .saturating_add(f[12])
            .saturating_add(f[13])
            .saturating_add(f[14]);
        Some(CpuTicks { total, idle, own })
    }

    /// Per-arm validity guard: **FOREIGN** CPU consumption during the arm's timed
    /// region, as a fraction of the whole box.
    ///
    /// Why not the 1-minute load average per arm: `KWayMerger` runs one producer
    /// thread per generation, so the run's OWN parallelism pushes `loadavg` past any
    /// useful ceiling by k = 10 — a per-arm loadavg gate fails on the instrument
    /// itself, not on interference (observed while building this, issue #2043). The
    /// foreign-CPU figure subtracts this process's own utime+stime and is therefore
    /// self-immune: it answers exactly the question the ceiling exists for, "was
    /// anything ELSE running while this arm was timed?". `loadavg` is still gated
    /// ONCE at run start (the machine must be quiet before the instrument adds its
    /// own load) and sampled per arm for the record, but only reported thereafter.
    #[derive(Debug)]
    struct CpuWatch {
        cores: f64,
        /// Enforced per-arm foreign-CPU ceiling in cores, derived from `cores`
        /// ([`foreign_cpu_ceiling`]) and printed in the header + per arm.
        foreign_ceiling: f64,
        /// Enforced run-start 1-minute load ceiling, derived from `cores`
        /// ([`load_ceiling`]) and printed in the header.
        load_ceiling: f64,
        max_foreign: f64,
        samples: usize,
        load_min: f64,
        load_max: f64,
        load_samples: usize,
    }

    impl CpuWatch {
        fn new() -> Self {
            let cores = std::thread::available_parallelism().map_or(1.0, |n| n.get() as f64);
            Self {
                cores,
                foreign_ceiling: foreign_cpu_ceiling(cores),
                load_ceiling: load_ceiling(cores),
                max_foreign: 0.0,
                samples: 0,
                load_min: f64::MAX,
                load_max: f64::MIN,
                load_samples: 0,
            }
        }

        /// Record a 1-minute load sample (informational only — see the struct docs).
        fn sample_load(&mut self) -> Option<f64> {
            let load = load_avg_1m();
            if let Some(l) = load {
                self.load_min = self.load_min.min(l);
                self.load_max = self.load_max.max(l);
                self.load_samples += 1;
            }
            load
        }

        /// Fail closed unless FOREIGN CPU over `before..now` stayed under the
        /// host-derived `foreign_ceiling`. Returns the measured foreign-core figure
        /// for the arm's observables line.
        fn check_foreign(&mut self, site: &str, before: Option<CpuTicks>) -> Option<f64> {
            let (before, after) = (before?, cpu_ticks()?);
            let total = after.total.saturating_sub(before.total);
            if total == 0 {
                return None;
            }
            let busy = total.saturating_sub(after.idle.saturating_sub(before.idle));
            let own = after.own.saturating_sub(before.own);
            // Foreign busy ticks: everything busy that was not us. Saturating, since
            // `/proc/stat` and `/proc/self/stat` are sampled a few µs apart.
            let foreign_cores = busy.saturating_sub(own) as f64 / total as f64 * self.cores;
            self.max_foreign = self.max_foreign.max(foreign_cores);
            self.samples += 1;
            let ceiling = self.foreign_ceiling;
            if !load_opt_out() && foreign_cores > ceiling {
                panic!(
                    "reconcile_overlap: {foreign_cores:.2} cores of FOREIGN CPU were busy while \
                     {site} was timed, over the {ceiling:.2}-core ceiling ({} cores) — that \
                     arm's number is void. Quiesce the machine and re-run, or set \
                     CQLITE_BENCH_ALLOW_LOAD=1 to smoke-run with discarded numbers.",
                    self.cores as u64
                );
            }
            Some(foreign_cores)
        }

        /// Print the run's validity summary: the worst foreign-CPU sample (the gate)
        /// and the loadavg range (informational, inflated by our own producers).
        fn report(&self) {
            if self.samples == 0 {
                println!(
                    "reconcile_overlap: foreign-CPU samples=0 (probe unavailable — non-Linux \
                     host); this run is NOT a valid measurement"
                );
            } else {
                println!(
                    "reconcile_overlap: foreign_cpu_cores samples={} max={:.2} \
                     ceiling={:.2} cores={:.0}",
                    self.samples, self.max_foreign, self.foreign_ceiling, self.cores
                );
            }
            if self.load_samples > 0 {
                // The producer count is `KWayMerger`'s one thread per generation at
                // the matrix's LARGEST k — derived from `K_VALUES`, never a literal,
                // so editing the matrix can never emit a wrong figure into the
                // operator-facing validity summary the record quotes (roborev,
                // issue #2043).
                let max_k = K_VALUES.iter().copied().max().unwrap_or(0);
                println!(
                    "reconcile_overlap: load1m samples={} min={:.2} max={:.2} spread={:.2} \
                     (INFORMATIONAL — includes this run's own {max_k} producer threads at \
                     k={max_k})",
                    self.load_samples,
                    self.load_min,
                    self.load_max,
                    self.load_max - self.load_min,
                );
            }
        }
    }

    /// Print the run header (machine spec + load) and fail closed when the machine
    /// is not quiesced BEFORE the instrument adds any load of its own: numbers taken
    /// on a busy box are void, so they must not be produced at all. Per-arm validity
    /// is then enforced by [`CpuWatch::check_foreign`], which is immune to this run's
    /// own threads. `CQLITE_BENCH_ALLOW_LOAD=1` opts out of both, visibly.
    fn assert_quiesced(watch: &mut CpuWatch) {
        let allow = load_opt_out();
        // Both ceilings are DERIVED from this host's core count, so the header
        // prints the values actually enforced for this run (roborev, issue #2043).
        let (cores, load_ceiling, foreign_ceiling) =
            (watch.cores, watch.load_ceiling, watch.foreign_ceiling);
        println!(
            "reconcile_overlap: cores={cores:.0} load_ceiling={load_ceiling:.2} \
             foreign_cpu_ceiling_cores={foreign_ceiling:.2} allow_load={allow}"
        );
        if allow {
            println!(
                "reconcile_overlap: CQLITE_BENCH_ALLOW_LOAD=1 — results are NOT a valid \
                 measurement and must not be substituted into the derate"
            );
        }
        let load = watch.sample_load();
        let load_str = load.map_or_else(|| "unavailable".to_string(), |l| format!("{l:.2}"));
        println!("reconcile_overlap: load1m at run-start = {load_str}");
        if allow {
            return;
        }
        match load {
            Some(l) if l <= load_ceiling => {}
            _ => panic!(
                "reconcile_overlap: 1-minute load average {load_str} at run start exceeds the \
                 {load_ceiling:.2} ceiling for this {cores:.0}-core host (or is unavailable — the \
                 probe is Linux-only) — a measurement taken here is void. Quiesce the machine and \
                 re-run, or set CQLITE_BENCH_ALLOW_LOAD=1 to smoke-run with discarded numbers."
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

        group.throughput(Throughput::Elements(probe.output_rows));
        let schema = fx.schema.clone();
        let timed = probe.clone();
        // Foreign-CPU window: opened immediately before the TIMED region and closed
        // immediately after, so the validity check covers exactly what Criterion
        // measured (fixture build and probe are untimed scaffolding).
        let cpu_before = cpu_ticks();
        group.bench_function(BenchmarkId::new(id_group, id_param), |b| {
            b.iter(|| {
                let stats = drain(&readers, &schema, now_secs);
                // Every timed iteration must do the SAME real work as the probe — a
                // 0-row or drifting drain can never pass as a measurement.
                assert_eq!(stats, timed, "{arm_id}: drain diverged");
                black_box(stats.output_rows)
            });
        });
        let foreign = watch.check_foreign(&arm_id, cpu_before);
        println!(
            "reconcile_overlap/{arm_id}: foreign_cpu_cores={} (ceiling {:.2})",
            foreign.map_or_else(|| "unavailable".to_string(), |f| format!("{f:.3}")),
            watch.foreign_ceiling
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
        let specs = arm_specs();

        let mut group = c.benchmark_group("reconcile_overlap");
        group
            .sample_size(20)
            .warm_up_time(Duration::from_secs(1))
            .measurement_time(Duration::from_secs(5));

        // `--list` enumeration must be FREE. This target's fixture synthesis, probe
        // drain and per-generation drains run OUTSIDE `bench_function` (they produce
        // the census that makes a timed number trustworthy), so a naive list run
        // would flush all 27 arms' generations and could even fail closed in
        // `assert_quiesced`/`check_foreign` while measuring nothing (roborev, issue
        // #2043). Register the ids from the SAME `arm_specs()` the measuring path
        // uses — criterion never executes a routine in list mode, and this branch is
        // unreachable in any measuring run (`--test` included), so nothing measured
        // changes.
        if list_only() {
            println!(
                "reconcile_overlap: --list — enumerating {} arm ids only; no fixture is built, \
                 no validity gate runs and nothing is measured",
                specs.len()
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
        assert_quiesced(&mut watch);

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
        watch.report();
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
