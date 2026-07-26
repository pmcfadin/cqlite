//! Measurement-validity guard for `benches/reconcile_overlap.rs` (issue #2043).
//!
//! A measurement taken while something ELSE was on the CPU is not a measurement,
//! so this module is the instrument's fail-closed gate. It is a separate
//! `#[path]`-included module (not part of `fixtures/mod.rs`, which every bench
//! target includes) so only the one target that needs it compiles it.
//!
//! ## Two tiers, both FAIL CLOSED — including when the probe itself is unreadable
//!
//! 1. **Run start:** the 1-minute load average must be ≤ [`load_ceiling`] before
//!    the instrument adds any load of its own ([`assert_quiesced`]).
//! 2. **Every timed interval:** the **foreign** (not-this-process) CPU busy over
//!    that interval must stay under [`foreign_cpu_ceiling`]
//!    ([`CpuWatch::observe_interval`]).
//!
//! "Fails closed" is literal here, and it is the property this module exists to
//! guarantee (roborev, issue #2043 — the earlier version returned `None` on an
//! unreadable `/proc` and the arm's Criterion number was still published as if
//! validated): an **unreadable** probe PANICS exactly like an over-ceiling
//! sample, and [`CpuWatch::end_arm`] additionally refuses an arm that was not
//! actually gated — it requires a minimum number of gated intervals AND that the
//! gated intervals cover ≥ [`MIN_GATED_WALL_FRACTION`] of that arm's timed wall
//! clock. The caller then asserts that EVERY arm passed through `end_arm`
//! ([`CpuWatch::arms_gated`]), so a skipped sample can never pass as a gated arm.
//!
//! ## Why the foreign-CPU window is per INTERVAL, not per arm
//!
//! Sampling once around a whole `bench_function` measures a **mean** over ≥6 s
//! (warm-up plus every sample batch), and a mean hides exactly the events that
//! void a sample: a 0.5 s four-core burst averages to ~0.33 cores over 6 s and
//! passes a 1.00-core ceiling while easily perturbing the batch it landed in
//! (roborev, issue #2043). The bench therefore drives Criterion through
//! `iter_custom` and calls [`CpuWatch::observe_interval`] once per **batch**; the
//! gate is the per-interval **maximum**, and the mean is kept as reported context.
//!
//! ## Why not a per-arm loadavg gate
//!
//! `KWayMerger` runs one producer thread per generation, so the run's OWN
//! parallelism pushes `loadavg` past any useful ceiling by k = 10 — a per-arm
//! loadavg gate fails on the instrument itself rather than on interference. The
//! foreign-CPU figure subtracts this process's own `utime+stime` and is therefore
//! self-immune. `loadavg` is gated ONCE at run start and thereafter only reported.
//!
//! ## Core count comes from the SAME source as the busy figure
//!
//! `busy`/`idle` are host-wide (`/proc/stat`), so the divisor that turns a busy
//! FRACTION into CORES must be host-wide too: it is the count of `cpuN` lines in
//! `/proc/stat`, not `available_parallelism()` (which is cgroup/affinity-aware and
//! in a CPU-limited container disagrees, so a run could be refused for host noise
//! it never competed with — roborev, issue #2043). The two are both printed, the
//! header says so when they DISAGREE, and the ceilings are scaled by the SMALLER
//! of the two so a restricted container never gets more absolute slack than the
//! CPU it can actually use.
//!
//! **Platform restriction:** both probes read `/proc`, so this instrument is
//! Linux-only by construction; anywhere else it can only produce a self-labelled
//! `CQLITE_BENCH_ALLOW_LOAD=1` non-measurement.

#![cfg(feature = "write-support")]

use std::time::Duration;

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
const LOAD_CEILING_PER_CORE: f64 = 0.125;

/// Floor for [`load_ceiling`] on very small hosts: at 1–4 cores the per-core budget
/// (0.125–0.5) would sit inside the noise of an otherwise-idle box's own loadavg
/// decay, so the gate would fail on nothing. 0.5 keeps a 4-core host at the same
/// 12.5 % it derives anyway and never goes stricter than that.
const LOAD_CEILING_FLOOR: f64 = 0.5;

/// Foreign-CPU budget per CORE, in cores: 1/16 core per core is ~6 % of the box —
/// enough for an ssh/editor session, far below anything that perturbs a merge drain.
/// Derived for the same reason as [`LOAD_CEILING_PER_CORE`]: a fixed `1.0` core is
/// 6 % of a 16-core box but **25 %** of a 4-core one. See [`foreign_cpu_ceiling`];
/// on the 16-core reference box it is exactly the `1.00` the banked runs enforced.
const FOREIGN_CPU_CEILING_PER_CORE: f64 = 0.0625;

/// Floor for [`foreign_cpu_ceiling`], in cores — a tiny host still gets a quarter
/// core of slack, below which sampling jitter alone would void arms.
const FOREIGN_CPU_CEILING_FLOOR: f64 = 0.25;

/// Minimum `/proc/stat` tick advance for an interval to be GATEABLE at all.
///
/// `USER_HZ` is 100, so a host tick is 10 ms of ONE cpu's capacity; with NOHZ idle
/// accounting a very short interval can legitimately show a zero (or 1–2 tick)
/// advance, from which no foreign-CPU figure can be computed. Criterion's own
/// sample batches are ≥ `measurement_time / sample_size` (250 ms with this bench's
/// configuration) and clear this bar by ~2 orders of magnitude; the intervals that
/// do not are Criterion's first few warm-up probes. They are COUNTED as ungated and
/// bounded by [`MIN_GATED_WALL_FRACTION`], never silently ignored.
const MIN_GATEABLE_TOTAL_TICKS: u64 = 8;

/// Fraction of an arm's TIMED wall clock that MUST have been covered by gated
/// intervals for the arm to count as validated.
const MIN_GATED_WALL_FRACTION: f64 = 0.90;

/// Minimum number of gated intervals per arm. This bench runs 20 Criterion samples
/// per arm, so anything near or below half of that means the sampling loop is not
/// shaped the way the gate assumes.
const MIN_GATED_INTERVALS: usize = 10;

/// Timed wall clock above which this process's OWN CPU ticks MUST have advanced.
///
/// The own-tick figure is subtracted from host busy to get "foreign", so an
/// own-tick extraction that silently yields 0 would count all of our own merge CPU
/// as foreign (roborev, issue #2043). After a second or more of a CPU-bound merge
/// drain, `utime+stime` cannot be flat — so a flat reading is a broken probe, and
/// [`CpuWatch::end_arm`] fails closed on it.
const OWN_TICK_SELF_CHECK_WALL: Duration = Duration::from_secs(1);

/// The enforced 1-minute load ceiling on a host with `cores` cores.
pub fn load_ceiling(cores: f64) -> f64 {
    (LOAD_CEILING_PER_CORE * cores).max(LOAD_CEILING_FLOOR)
}

/// The enforced per-interval FOREIGN-CPU ceiling, in whole cores, on a host with
/// `cores` cores. Exceeding it VOIDS the arm.
pub fn foreign_cpu_ceiling(cores: f64) -> f64 {
    (FOREIGN_CPU_CEILING_PER_CORE * cores).max(FOREIGN_CPU_CEILING_FLOOR)
}

/// `CQLITE_BENCH_ALLOW_LOAD=1` — the visible opt-out that turns the run into a
/// self-labelled NON-measurement. Under it every check below still COMPUTES and
/// PRINTS, but nothing panics.
pub fn load_opt_out() -> bool {
    std::env::var("CQLITE_BENCH_ALLOW_LOAD").as_deref() == Ok("1")
}

/// Read `/proc/loadavg`'s 1-minute figure. **Linux-only**: `None` on any other
/// platform (or an unreadable proc) — treated as "unknown", which fails closed
/// exactly like an over-ceiling load.
fn load_avg_1m() -> Option<f64> {
    let raw = std::fs::read_to_string("/proc/loadavg").ok()?;
    raw.split_whitespace().next()?.parse::<f64>().ok()
}

/// This process's `utime + stime + cutime + cstime` in USER_HZ ticks, extracted
/// from one `/proc/self/stat` line.
///
/// A PURE function with an explicit self-test ([`assert_probe_parsers_sound`])
/// rather than an inline hand-counted offset with a per-field
/// `.parse().unwrap_or(0)` (roborev, issue #2043): under `unwrap_or(0)` a wrong
/// index or an unparseable field silently yields `own = 0`, which makes ALL of this
/// process's own merge CPU count as FOREIGN and voids arms for the instrument's own
/// work. Here every field that is read is parsed with `?`, so anything unexpected
/// returns `None` — which the callers treat as an unreadable probe and fail closed.
///
/// Field numbering (`proc(5)`, 1-based): `pid comm state ppid pgrp session tty_nr
/// tpgid flags minflt cminflt majflt cmajflt utime stime cutime cstime` — `utime`
/// is field 14. `comm` may itself contain spaces and parentheses, so the scan
/// starts after the LAST `)`; the first token after it is `state`, a single
/// character (deliberately validated, never parsed as a number), which puts
/// `utime` at index 10 of the tokens that follow.
pub fn own_ticks_from(stat_line: &str) -> Option<u64> {
    let tail = &stat_line[stat_line.rfind(')')? + 1..];
    let mut tokens = tail.split_whitespace();
    // `state` — one character (`R`/`S`/`D`/...). Validated rather than parsed: it
    // is the token whose non-numeric-ness the old `unwrap_or(0)` was silently
    // absorbing, which is what made a real parse failure invisible.
    let state = tokens.next()?;
    if state.len() != 1 || !state.chars().all(|c| c.is_ascii_alphabetic()) {
        return None;
    }
    let fields: Vec<&str> = tokens.collect();
    // utime, stime, cutime, cstime (fields 14..=17 ⇒ indices 10..=13 here).
    let mut ticks: u64 = 0;
    for idx in 10..=13 {
        ticks = ticks.saturating_add(fields.get(idx)?.parse::<u64>().ok()?);
    }
    Some(ticks)
}

/// Host cpu count taken from the SAME file as the busy figure: the number of
/// per-cpu `cpuN` lines in `/proc/stat`. See the module docs on why
/// `available_parallelism()` is not the right divisor for a host-wide fraction.
fn host_cpu_count(stat: &str) -> Option<f64> {
    let n = stat
        .lines()
        .filter(|l| {
            l.strip_prefix("cpu")
                .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
        })
        .count();
    (n > 0).then_some(n as f64)
}

/// Cumulative CPU-tick counters for the machine and for THIS process, read from
/// `/proc/stat` + `/proc/self/stat` (Linux-only, same USER_HZ unit — only ratios
/// are taken, so the tick length never needs to be known).
#[derive(Debug, Clone, Copy)]
pub struct CpuTicks {
    /// All jiffies across all CPUs (capacity of the box over the interval).
    total: u64,
    /// Idle + iowait jiffies.
    idle: u64,
    /// This process's user+system jiffies, children included.
    own: u64,
}

/// Sample both tick counters, or `None` if either `/proc` file is unreadable or
/// shaped unexpectedly. `None` is NEVER "assume fine": callers fail closed on it.
pub fn cpu_ticks() -> Option<CpuTicks> {
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
    let own = own_ticks_from(&std::fs::read_to_string("/proc/self/stat").ok()?)?;
    Some(CpuTicks { total, idle, own })
}

/// What one arm's gated intervals observed — the arm's line in the record.
#[derive(Debug, Clone, Copy)]
pub struct ArmForeign {
    /// Worst per-interval foreign-CPU figure, in cores. THIS is what was gated.
    pub max_cores: f64,
    /// Mean foreign CPU over the arm's gated intervals, in cores (context only).
    pub mean_cores: f64,
    /// This process's own CPU over the arm's gated intervals, in cores (context;
    /// its being > 0 is the own-tick probe self-check).
    pub own_cores: f64,
    /// Gated intervals, and intervals seen in total (the difference is Criterion's
    /// sub-[`MIN_GATEABLE_TOTAL_TICKS`] warm-up probes).
    pub gated_intervals: usize,
    pub total_intervals: usize,
    /// Fraction of the arm's timed wall clock covered by gated intervals.
    pub gated_wall_fraction: f64,
}

/// Per-arm + whole-run foreign-CPU accounting and gating.
#[derive(Debug)]
pub struct CpuWatch {
    /// Host cpu count from `/proc/stat` — the divisor for the busy FRACTION.
    cores: f64,
    /// `available_parallelism()` — the cgroup/affinity-aware view, reported and
    /// used (as the smaller of the two) to scale the ceilings.
    visible_cores: f64,
    foreign_ceiling: f64,
    load_ceiling: f64,
    // Whole-run rollup.
    max_foreign: f64,
    foreign_sum: f64,
    intervals: usize,
    short_intervals: usize,
    arms_gated: usize,
    // Current arm.
    arm_max: f64,
    arm_sum: f64,
    arm_own_ticks: u64,
    arm_total_ticks: u64,
    arm_intervals: usize,
    arm_gated: usize,
    arm_wall: Duration,
    arm_gated_wall: Duration,
    // Loadavg (informational after run start).
    load_min: f64,
    load_max: f64,
    load_samples: usize,
}

impl CpuWatch {
    pub fn new() -> Self {
        let visible_cores = std::thread::available_parallelism().map_or(1.0, |n| n.get() as f64);
        let cores = std::fs::read_to_string("/proc/stat")
            .ok()
            .as_deref()
            .and_then(host_cpu_count)
            .unwrap_or(visible_cores);
        // Ceilings scale with the SMALLER view: a restricted container must not be
        // granted host-sized absolute slack (roborev, issue #2043).
        let ceiling_cores = cores.min(visible_cores);
        Self {
            cores,
            visible_cores,
            foreign_ceiling: foreign_cpu_ceiling(ceiling_cores),
            load_ceiling: load_ceiling(ceiling_cores),
            max_foreign: 0.0,
            foreign_sum: 0.0,
            intervals: 0,
            short_intervals: 0,
            arms_gated: 0,
            arm_max: 0.0,
            arm_sum: 0.0,
            arm_own_ticks: 0,
            arm_total_ticks: 0,
            arm_intervals: 0,
            arm_gated: 0,
            arm_wall: Duration::ZERO,
            arm_gated_wall: Duration::ZERO,
            load_min: f64::MAX,
            load_max: f64::MIN,
            load_samples: 0,
        }
    }

    pub fn foreign_ceiling(&self) -> f64 {
        self.foreign_ceiling
    }

    /// Arms that passed [`Self::end_arm`]'s validation. The caller asserts this
    /// equals the number of arms it ran.
    pub fn arms_gated(&self) -> usize {
        self.arms_gated
    }

    /// Record a 1-minute load sample (informational after run start).
    pub fn sample_load(&mut self) -> Option<f64> {
        let load = load_avg_1m();
        if let Some(l) = load {
            self.load_min = self.load_min.min(l);
            self.load_max = self.load_max.max(l);
            self.load_samples += 1;
        }
        load
    }

    /// Open a new arm's accounting. Must be paired with [`Self::end_arm`].
    pub fn begin_arm(&mut self) {
        self.arm_max = 0.0;
        self.arm_sum = 0.0;
        self.arm_own_ticks = 0;
        self.arm_total_ticks = 0;
        self.arm_intervals = 0;
        self.arm_gated = 0;
        self.arm_wall = Duration::ZERO;
        self.arm_gated_wall = Duration::ZERO;
    }

    /// Sample the counters at the START of one timed interval (one Criterion
    /// batch). `None` (unreadable `/proc`) is carried through to
    /// [`Self::observe_interval`], which fails closed on it.
    pub fn interval_start(&self) -> Option<CpuTicks> {
        cpu_ticks()
    }

    /// Close one timed interval: compute the FOREIGN CPU busy over it and gate on
    /// it. Panics (unless opted out) when the interval was over the ceiling OR when
    /// the probe could not be read at all — an ungated interval never passes
    /// silently.
    pub fn observe_interval(&mut self, site: &str, before: Option<CpuTicks>, wall: Duration) {
        self.arm_intervals += 1;
        self.arm_wall += wall;
        let after = cpu_ticks();
        let (before, after) = match (before, after) {
            (Some(b), Some(a)) => (b, a),
            _ => {
                self.void(
                    site,
                    "the /proc CPU probe was UNREADABLE across a timed interval, so foreign CPU \
                     for it is unknown",
                );
                return;
            }
        };
        let total = after.total.saturating_sub(before.total);
        if total < MIN_GATEABLE_TOTAL_TICKS {
            // Readable, but too short to resolve at 10 ms tick granularity. Counted,
            // and bounded by the gated-wall-fraction check in `end_arm`.
            self.short_intervals += 1;
            return;
        }
        let busy = total.saturating_sub(after.idle.saturating_sub(before.idle));
        let own = after.own.saturating_sub(before.own);
        // Foreign busy ticks: everything busy that was not us. Saturating, since
        // `/proc/stat` and `/proc/self/stat` are sampled a few µs apart.
        let foreign_cores = busy.saturating_sub(own) as f64 / total as f64 * self.cores;

        self.arm_max = self.arm_max.max(foreign_cores);
        self.arm_sum += foreign_cores;
        self.arm_own_ticks = self.arm_own_ticks.saturating_add(own);
        self.arm_total_ticks = self.arm_total_ticks.saturating_add(total);
        self.arm_gated += 1;
        self.arm_gated_wall += wall;
        self.max_foreign = self.max_foreign.max(foreign_cores);
        self.foreign_sum += foreign_cores;
        self.intervals += 1;

        let ceiling = self.foreign_ceiling;
        if foreign_cores > ceiling && !load_opt_out() {
            panic!(
                "reconcile_overlap: {foreign_cores:.2} cores of FOREIGN CPU were busy during a \
                 {:.0} ms timed interval of {site}, over the {ceiling:.2}-core ceiling ({:.0} \
                 cores) — that arm's number is void. Quiesce the machine and re-run, or set \
                 CQLITE_BENCH_ALLOW_LOAD=1 to smoke-run with discarded numbers.",
                wall.as_secs_f64() * 1e3,
                self.cores,
            );
        }
    }

    /// Close the arm and REFUSE it unless it was really gated: enough gated
    /// intervals, gated intervals covering ≥ [`MIN_GATED_WALL_FRACTION`] of the
    /// timed wall clock, and this process's own CPU actually advancing over a
    /// multi-second CPU-bound region.
    pub fn end_arm(&mut self, site: &str) -> ArmForeign {
        let fraction = if self.arm_wall.is_zero() {
            0.0
        } else {
            self.arm_gated_wall.as_secs_f64() / self.arm_wall.as_secs_f64()
        };
        if self.arm_gated < MIN_GATED_INTERVALS {
            self.void(
                site,
                &format!(
                    "only {} of {} timed intervals could be gated (minimum {MIN_GATED_INTERVALS})",
                    self.arm_gated, self.arm_intervals
                ),
            );
        } else if fraction < MIN_GATED_WALL_FRACTION {
            self.void(
                site,
                &format!(
                    "gated intervals cover only {:.1} % of the arm's {:.1} s timed wall clock \
                     (minimum {:.0} %)",
                    fraction * 100.0,
                    self.arm_wall.as_secs_f64(),
                    MIN_GATED_WALL_FRACTION * 100.0
                ),
            );
        } else if self.arm_wall >= OWN_TICK_SELF_CHECK_WALL && self.arm_own_ticks == 0 {
            self.void(
                site,
                &format!(
                    "this process's OWN CPU ticks did not advance across {:.1} s of CPU-bound \
                     timed merge drains — the /proc/self/stat own-tick probe is broken, so every \
                     tick of our own work was being counted as FOREIGN",
                    self.arm_wall.as_secs_f64()
                ),
            );
        }
        let own_cores = if self.arm_total_ticks == 0 {
            0.0
        } else {
            self.arm_own_ticks as f64 / self.arm_total_ticks as f64 * self.cores
        };
        self.arms_gated += 1;
        ArmForeign {
            max_cores: self.arm_max,
            mean_cores: if self.arm_gated == 0 {
                0.0
            } else {
                self.arm_sum / self.arm_gated as f64
            },
            own_cores,
            gated_intervals: self.arm_gated,
            total_intervals: self.arm_intervals,
            gated_wall_fraction: fraction,
        }
    }

    /// The single fail-closed exit: panic with `why` unless the run is a
    /// self-labelled non-measurement, in which case say so and continue.
    fn void(&self, site: &str, why: &str) {
        if load_opt_out() {
            println!(
                "reconcile_overlap/{site}: NOT GATED — {why} (allowed only because \
                 CQLITE_BENCH_ALLOW_LOAD=1; this run is not a measurement)"
            );
            return;
        }
        panic!(
            "reconcile_overlap: {site}'s measurement is VOID — {why}. A number that was not \
             gated must not be published, so this run fails closed. Re-run on a host with a \
             readable /proc, or set CQLITE_BENCH_ALLOW_LOAD=1 to smoke-run with discarded numbers."
        );
    }

    /// Print the run's validity summary: the worst per-interval foreign-CPU sample
    /// (the gate), the mean (context) and the loadavg range (informational,
    /// inflated by our own producers).
    pub fn report(&self, arms_run: usize, max_k: usize) {
        if self.intervals == 0 {
            println!(
                "reconcile_overlap: foreign-CPU intervals=0 (probe unavailable — non-Linux \
                 host); this run is NOT a valid measurement"
            );
        } else {
            println!(
                "reconcile_overlap: foreign_cpu_cores intervals={} max={:.3} mean={:.3} \
                 ceiling={:.2} cores={:.0} ungateable_short_intervals={}",
                self.intervals,
                self.max_foreign,
                self.foreign_sum / self.intervals as f64,
                self.foreign_ceiling,
                self.cores,
                self.short_intervals,
            );
        }
        println!(
            "reconcile_overlap: arms_gated={}/{arms_run}{}",
            self.arms_gated,
            if self.arms_gated == arms_run {
                ""
            } else {
                " — MISMATCH: an arm produced a number without being gated"
            }
        );
        if self.load_samples > 0 {
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

/// Print the run header (machine spec + load) and fail closed when the machine is
/// not quiesced BEFORE the instrument adds any load of its own: numbers taken on a
/// busy box are void, so they must not be produced at all. Per-interval validity is
/// then enforced by [`CpuWatch::observe_interval`], which is immune to this run's
/// own threads. `CQLITE_BENCH_ALLOW_LOAD=1` opts out of both, visibly.
pub fn assert_quiesced(watch: &mut CpuWatch) {
    let allow = load_opt_out();
    let (cores, visible, load_ceiling, foreign_ceiling) = (
        watch.cores,
        watch.visible_cores,
        watch.load_ceiling,
        watch.foreign_ceiling,
    );
    println!(
        "reconcile_overlap: cores={cores:.0} (proc_stat={cores:.0} \
         available_parallelism={visible:.0}) load_ceiling={load_ceiling:.2} \
         foreign_cpu_ceiling_cores={foreign_ceiling:.2} allow_load={allow}"
    );
    if (cores - visible).abs() > f64::EPSILON {
        println!(
            "reconcile_overlap: NOTE — /proc/stat reports {cores:.0} cpus but \
             available_parallelism() reports {visible:.0} (cgroup quota / cpu affinity). The \
             host-wide busy fraction is scaled by the /proc/stat count (same source as the busy \
             figure); the ceilings are scaled by the SMALLER of the two, so this run's \
             foreign-CPU budget is the stricter one."
        );
    }
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

/// Self-test of the two `/proc` parsers, over CAPTURED samples plus the live files.
///
/// Runs on EVERY invocation of the bench target (measuring, `--test` and `--list`
/// alike) rather than as `#[cfg(test)]`: this target is `harness = false`, so a
/// `#[test]` inside it would never be executed by `cargo test` — an "always
/// present, never run" test. Called before anything is measured, so a broken
/// extraction (the roborev finding this guards: a wrong positional offset silently
/// yielding `own = 0`) fails the run instead of quietly inflating foreign CPU.
pub fn assert_probe_parsers_sound() {
    // Captured `/proc/self/stat` (a `comm` containing BOTH a space and a `)`, and a
    // negative `tpgid`, so the offset arithmetic is exercised where it is fragile):
    // utime=111 stime=222 cutime=3 cstime=4 ⇒ 340.
    const SAMPLE: &str = "4242 (cargo bench (x)) S 4241 4242 4242 0 -1 4194304 5000 0 12 0 \
                          111 222 3 4 20 0 8 0 99999 123456789 4096 18446744073709551615 1 1";
    assert_eq!(
        own_ticks_from(SAMPLE),
        Some(340),
        "own_ticks_from must sum utime+stime+cutime+cstime of a captured /proc/self/stat line"
    );
    // Truncated before `cstime` ⇒ unknown, never a silent 0.
    assert_eq!(
        own_ticks_from("4242 (x) S 1 2 3 0 -1 0 0 0 0 0 111 222 3"),
        None,
        "a /proc/self/stat line too short to carry cstime must be UNKNOWN, not 0"
    );
    // Unparseable tick field ⇒ unknown, never a silent 0.
    assert_eq!(
        own_ticks_from("4242 (x) S 1 2 3 0 -1 0 0 0 0 0 abc 222 3 4 20 0 8"),
        None,
        "an unparseable tick field must be UNKNOWN, not 0"
    );
    // No `)` at all, and a non-`state` first token ⇒ unknown.
    assert_eq!(own_ticks_from("4242 x S 1 2 3"), None);
    assert_eq!(
        own_ticks_from("4242 (x) 99 1 2 3 0 -1 0 0 0 0 0 111 222 3 4"),
        None,
        "the token after comm must be the single-character `state` field"
    );
    // Captured `/proc/stat` head: four per-cpu lines ⇒ 4 cores, and `intr`/`cpu `
    // must not be miscounted as per-cpu lines.
    const STAT: &str = "cpu  1 2 3 4 5 6 7 8\ncpu0 1 0 0 0 0 0 0 0\ncpu1 1 0 0 0 0 0 0 0\n\
                        cpu2 1 0 0 0 0 0 0 0\ncpu3 1 0 0 0 0 0 0 0\nintr 9 9 9\nctxt 5\n";
    assert_eq!(
        host_cpu_count(STAT),
        Some(4.0),
        "host_cpu_count must count only per-cpu `cpuN` lines"
    );
    assert_eq!(host_cpu_count("cpu  1 2 3 4 5\nintr 1\n"), None);

    // Live probe: on the Linux hosts this instrument is valid on, both parsers must
    // succeed and the process must already have burned some CPU getting here.
    match std::fs::read_to_string("/proc/self/stat") {
        Ok(live) => assert!(
            own_ticks_from(&live).is_some(),
            "own_ticks_from failed on this host's LIVE /proc/self/stat: {live:?}"
        ),
        Err(e) => println!(
            "reconcile_overlap: /proc/self/stat unreadable ({e}) — this host can only produce a \
             CQLITE_BENCH_ALLOW_LOAD=1 non-measurement"
        ),
    }
}
