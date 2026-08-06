//! Bounded partition repeat-access probe (issue #2827).
//!
//! # What this delivers, and what it deliberately does not
//!
//! This module is **an instrument**. Together with the committed decision
//! procedure at `docs/research/decoded-partition-cache-decision.md` it delivers
//! *the instrument and the procedure, not the field number*. Issue #2827's
//! original AC2 — "decides whether a 64–128 MiB decoded-partition cache clears a
//! useful hit ratio" — is **not satisfied** by this code. It is not waived and not
//! deferred to another issue: it becomes satisfiable on the first real keyed
//! workload run with the probe enabled. The blocker is that **no field keyed
//! workload with captured concentration exists** (`docs/research/phase2-verify-caching.md:214-216`).
//! Nothing this module emits, and nothing any test here asserts, may be cited as a
//! measured field skew or as a go/no-go.
//!
//! # What it measures
//!
//! Over a tumbling measurement window it counts, per DISTINCT partition, how many
//! times that partition was read on the logical point-read path, and summarises
//! the result as a fixed-cardinality histogram over exactly six repeat buckets
//! (`1 | 2 | 3-4 | 5-8 | 9-16 | 17+`) plus the distinct-partition on-disk bytes in
//! each bucket. That is the concentration SHAPE a decoded-partition cache's sizing
//! math needs, with **no per-key attribute of any kind** — the binding constraint
//! from `docs/observability/configuration.md` ("Unbounded values … **partition
//! keys** … are **NEVER** attached as attributes or span fields") is why the
//! summarisation happens in-process and only bucket labels leave it.
//!
//! # Cost
//!
//! - **Disabled (the default): zero.** No allocation at all — the counting table is
//!   allocated lazily on the first recorded access — and the hot path is one
//!   relaxed atomic load.
//! - **Enabled: exactly 3 MiB, fixed** — no term in partition count, in qps, in
//!   window length, or in the sampling scale. See [`table`].
//!
//! # Surfaces
//!
//! - [`record_partition_access`] — one call per LOGICAL partition read (never per
//!   SSTable probe: a partition living in *k* generations would otherwise report a
//!   repeat count of *k* for a single read, manufacturing concentration the
//!   workload does not have).
//! - [`close_window`] — closes the current window deterministically and emits. It
//!   is public precisely so tests never have to sleep on a clock.
//! - [`PartitionAccessRecorder`] — an owned recorder, so a test can drive a whole
//!   window without touching process-global state.
//! - [`decision`] — the executable form of the committed decision procedure.

pub mod decision;
mod table;
mod types;

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use super::{catalog, AttrValue};
use table::{Insert, Table};

pub use decision::{Refusal, Verdict};
pub use types::{
    AccessWeight, AccessWeightBuilder, BucketStats, RepeatBucket, SizeSource, WindowSummary,
};

/// Environment variable that turns the probe on. Default OFF.
const PROBE_ENV: &str = "CQLITE_PARTITION_ACCESS_PROBE";

/// Effective-state cache so the disabled hot path is ONE relaxed atomic load.
/// `0` = not yet resolved, `1` = on, `2` = off.
static EFFECTIVE: AtomicU8 = AtomicU8::new(0);
const STATE_UNRESOLVED: u8 = 0;
const STATE_ON: u8 = 1;
const STATE_OFF: u8 = 2;

/// Parse a `CQLITE_PARTITION_ACCESS_PROBE` value.
///
/// A pure function so the parse is unit-testable without touching the process
/// environment (the `now_clock::now_from` / `parse_read_path_mode` pattern).
/// Returns `None` for an unrecognised value — the caller reports that LOUDLY and
/// leaves the probe off, rather than silently treating a typo'd knob as "on".
pub fn parse_probe_flag(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" | "enabled" => Some(true),
        "0" | "false" | "off" | "no" | "disabled" | "" => Some(false),
        _ => None,
    }
}

/// The raw env value, read ONCE per process.
fn cached_env() -> Option<&'static str> {
    static ENV: OnceLock<Option<String>> = OnceLock::new();
    ENV.get_or_init(|| std::env::var(PROBE_ENV).ok()).as_deref()
}

fn resolve_from_env() -> bool {
    match cached_env() {
        None => false,
        Some(raw) => match parse_probe_flag(raw) {
            Some(v) => v,
            None => {
                // Loud, once (the env read itself is memoised, so this branch runs
                // at most once per process). A mistyped knob that silently no-ops
                // would defeat the knob's purpose.
                tracing::error!(
                    value = raw,
                    "unrecognised {PROBE_ENV} value — the partition access-distribution \
                     probe stays OFF; accepted values are 1/true/on/yes/enabled and \
                     0/false/off/no/disabled"
                );
                false
            }
        },
    }
}

/// Whether the probe is currently recording.
///
/// Steady-state cost is one relaxed atomic load. Off unless
/// `CQLITE_PARTITION_ACCESS_PROBE` says otherwise or a caller set
/// [`set_probe_enabled`].
#[inline]
pub fn enabled() -> bool {
    match EFFECTIVE.load(Ordering::Relaxed) {
        STATE_ON => true,
        STATE_OFF => false,
        _ => {
            let on = resolve_from_env();
            EFFECTIVE.store(if on { STATE_ON } else { STATE_OFF }, Ordering::Relaxed);
            on
        }
    }
}

/// Programmatically turn the probe on or off, taking precedence over the
/// environment (the `CQLITE_READ_PATH` config-over-env precedence pattern).
///
/// `Some(true)`/`Some(false)` pin the state; `None` returns the process to
/// resolving from the environment on the next [`enabled`] call.
pub fn set_probe_enabled(state: Option<bool>) {
    let v = match state {
        Some(true) => STATE_ON,
        Some(false) => STATE_OFF,
        None => STATE_UNRESOLVED,
    };
    EFFECTIVE.store(v, Ordering::Relaxed);
}

/// How a window's close was triggered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WindowConfig {
    /// Wall-clock length of a window. Checked on record; never asserted on by a
    /// correctness test.
    pub duration: Duration,
    /// Recorded-access bound; closes the window before the sample degrades on a
    /// workload far above the design rate.
    pub max_accesses: u64,
    /// Sampling-prefix cap. Once the recorder has widened the admission predicate
    /// this far and the table is STILL at its load factor, the surviving sample is
    /// too small to mean anything: the window is marked non-census and the decision
    /// procedure refuses it.
    ///
    /// Configurable only so the floor is reachable in a test. At the production
    /// default of [`DEFAULT_MAX_PREFIX_BITS`] the sample is 1-in-1,048,576, which
    /// no realistic corpus reaches — a property worth keeping, and a scenario worth
    /// being able to exercise.
    pub max_prefix_bits: u32,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(60),
            max_accesses: 5_000_000,
            max_prefix_bits: DEFAULT_MAX_PREFIX_BITS,
        }
    }
}

/// What the fast path decided the write-locked slow path must do.
#[derive(Clone, Copy, Debug)]
enum SlowPath {
    /// Nothing — the access was handled entirely under the read lock.
    None,
    /// The table does not exist yet.
    Allocate,
    /// The table is at its load factor (or gave up probing); widen the sampling
    /// prefix, replaying the triggering access only if it is not yet counted.
    Downsample {
        /// Whether the triggering access still needs recording.
        replay: bool,
    },
}

struct WindowState {
    table: Table,
    prefix_bits: u32,
    at_sampling_floor: bool,
    started: Instant,
}

/// Production sampling-prefix cap. At `k = 20` the sample is 1-in-1,048,576, which
/// over a field-scale corpus admits a couple of keys — statistically worthless, so
/// the window is marked non-census and the decision procedure refuses it.
pub const DEFAULT_MAX_PREFIX_BITS: u32 = 20;

/// A bounded partition repeat-access recorder.
///
/// Owned instances exist so tests can drive an entire window with no
/// process-global state and no cross-test interference; the process-global
/// instance behind [`record_partition_access`] is one of these.
pub struct PartitionAccessRecorder {
    state: RwLock<Option<WindowState>>,
    recorded: AtomicU64,
    /// Accesses the recorder was asked to record but could NOT land in the table —
    /// the table was at its load factor with the sampling prefix already at its cap,
    /// so no slot could be claimed. Reported on the closed window
    /// ([`WindowSummary::dropped_accesses`]) rather than lost silently.
    dropped: AtomicU64,
    /// Window-close policy, held as atomics rather than a plain field so
    /// [`PartitionAccessRecorder::set_window_config`] can retune the PROCESS-GLOBAL
    /// recorder — which is otherwise unreachable behind a `OnceLock` — without
    /// putting a lock on the record path.
    duration_nanos: AtomicU64,
    max_accesses: AtomicU64,
    max_prefix_bits: AtomicU32,
}

impl Default for PartitionAccessRecorder {
    fn default() -> Self {
        Self::new(WindowConfig::default())
    }
}

impl PartitionAccessRecorder {
    /// A recorder that has allocated nothing. The 3 MiB table appears on the first
    /// recorded access and never grows.
    pub fn new(config: WindowConfig) -> Self {
        let recorder = Self {
            state: RwLock::new(None),
            recorded: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            duration_nanos: AtomicU64::new(0),
            max_accesses: AtomicU64::new(0),
            max_prefix_bits: AtomicU32::new(0),
        };
        recorder.set_window_config(config);
        recorder
    }

    /// Replace the window-close policy.
    ///
    /// Exists because the process-global recorder ([`global`]) is created behind a
    /// `OnceLock` with [`WindowConfig::default`], so an end-to-end test driving the
    /// real read path cannot otherwise reach it — and with the default 60 s duration
    /// such a test depends on WALL CLOCK: a stalled or CPU-starved run whose reads
    /// straddle the boundary auto-closes the window mid-flow and the assertions
    /// evaporate. Tests set an unreachable duration and close explicitly.
    ///
    /// Takes effect from the next recorded access; it does not close the open
    /// window.
    pub fn set_window_config(&self, config: WindowConfig) {
        self.duration_nanos.store(
            u64::try_from(config.duration.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.max_accesses
            .store(config.max_accesses, Ordering::Relaxed);
        self.max_prefix_bits
            .store(config.max_prefix_bits, Ordering::Relaxed);
    }

    /// The window-close policy currently in force.
    pub fn window_config(&self) -> WindowConfig {
        WindowConfig {
            duration: Duration::from_nanos(self.duration_nanos.load(Ordering::Relaxed)),
            max_accesses: self.max_accesses.load(Ordering::Relaxed),
            max_prefix_bits: self.max_prefix_bits.load(Ordering::Relaxed),
        }
    }

    /// Current footprint in bytes: `0` before the first recorded access, and the
    /// fixed table size thereafter — never a function of the partition count.
    pub fn footprint_bytes(&self) -> usize {
        match self.state.read() {
            Ok(g) => g.as_ref().map_or(0, |s| s.table.footprint_bytes()),
            Err(poisoned) => poisoned
                .into_inner()
                .as_ref()
                .map_or(0, |s| s.table.footprint_bytes()),
        }
    }

    /// The fixed footprint an ENABLED recorder occupies, independent of workload.
    pub const fn declared_footprint_bytes() -> usize {
        table::TABLE_BYTES
    }

    /// Record one LOGICAL partition access.
    ///
    /// `key` is the raw partition-key bytes the read path already holds; it is
    /// hashed for slot addressing and within-window identity and is never stored,
    /// logged, or emitted.
    ///
    /// Returns the summary of a window this access happened to CLOSE (a duration or
    /// access-count trigger firing), so the global wrapper can emit it.
    pub fn record(&self, key: &[u8], weight: AccessWeight) -> Option<WindowSummary> {
        let hash = table::hash_key(key);
        let bytes = weight.bytes();
        let flags = match weight.source() {
            SizeSource::SuccessorGap => table::FLAG_SIZE_FROM_GAP,
            SizeSource::Index | SizeSource::Unavailable => 0,
        };
        self.recorded.fetch_add(1, Ordering::Relaxed);

        // Fast path: shared read lock, relaxed atomics on one slot.
        let mut slow = SlowPath::None;
        {
            let guard = self.read_state();
            if let Some(state) = guard.as_ref() {
                match state
                    .table
                    .record_with_flags(hash, state.prefix_bits, bytes, flags)
                {
                    // The access is ALREADY counted. The slow path must only
                    // widen the sampling prefix — re-recording here would count
                    // this one access twice and shift its partition a bucket to
                    // the right, manufacturing concentration out of nothing.
                    Insert::Recorded => {
                        if state.table.occupancy() >= table::LOAD_FACTOR_LIMIT {
                            slow = SlowPath::Downsample { replay: false };
                        }
                    }
                    Insert::NotAdmitted => {}
                    // Not counted: make room, then replay it.
                    Insert::Full => slow = SlowPath::Downsample { replay: true },
                }
            } else {
                slow = SlowPath::Allocate;
            }
        }

        match slow {
            SlowPath::None => {}
            SlowPath::Allocate => self.grow_or_downsample(hash, bytes, flags, true),
            SlowPath::Downsample { replay } => self.grow_or_downsample(hash, bytes, flags, replay),
        }
        self.close_if_triggered()
    }

    /// Allocate the table on first use, or widen the sampling prefix when the
    /// table is at its load factor. Takes the WRITE lock; both events are rare.
    ///
    /// `replay` says whether the triggering access still needs to be counted. It is
    /// `false` when the fast path already counted it (the load-factor case) and
    /// `true` when it did not (first allocation, or a probe run that found no
    /// slot).
    fn grow_or_downsample(&self, hash: u64, bytes: Option<u64>, flags: u8, replay: bool) {
        let mut guard = self.write_state();
        match guard.as_mut() {
            None => {
                let state = WindowState {
                    table: Table::new(),
                    prefix_bits: 0,
                    at_sampling_floor: false,
                    started: Instant::now(),
                };
                if replay {
                    state.table.record_with_flags(hash, 0, bytes, flags);
                }
                *guard = Some(state);
            }
            Some(state) => {
                // Widen until the table is back under its load factor, or until the
                // sampling floor is reached (at which point the window stops
                // admitting new keys — survivors keep counting so the window stays
                // internally consistent).
                while state.table.occupancy() >= table::LOAD_FACTOR_LIMIT
                    && state.prefix_bits < self.max_prefix_bits.load(Ordering::Relaxed)
                {
                    state.prefix_bits += 1;
                    state.table.downsample(state.prefix_bits);
                }
                // The floor is a property of the SCALE REACHED, not of whether the
                // last widen happened to get under the load factor. Gating this on
                // occupancy too would let a window that widened all the way to the
                // cap and then dropped below the factor report
                // `at_sampling_floor = false` beside a 1-in-2^cap denominator — and
                // the decision procedure would price that sample. D4/D7 and the
                // spec are unconditional: a window at the cap is refused.
                if state.prefix_bits >= self.max_prefix_bits.load(Ordering::Relaxed) {
                    state.at_sampling_floor = true;
                }
                // Re-attempt the access ONLY if the fast path did not already count
                // it. A `Full` here — an unlucky long probe run, or a table still at
                // its load factor because the prefix cannot widen further — DROPS
                // the access, so it must be counted where a reader can see it: a
                // measurement instrument that silently loses input is worse than one
                // that admits it did.
                if replay
                    && matches!(
                        state
                            .table
                            .record_with_flags(hash, state.prefix_bits, bytes, flags),
                        Insert::Full
                    )
                {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    fn close_if_triggered(&self) -> Option<WindowSummary> {
        let (elapsed, over_count) = {
            let guard = self.read_state();
            let state = guard.as_ref()?;
            (
                u64::try_from(state.started.elapsed().as_nanos()).unwrap_or(u64::MAX)
                    >= self.duration_nanos.load(Ordering::Relaxed),
                self.recorded.load(Ordering::Relaxed) >= self.max_accesses.load(Ordering::Relaxed),
            )
        };
        if elapsed || over_count {
            self.close_window()
        } else {
            None
        }
    }

    /// Close the current window DETERMINISTICALLY: bucket every live entry, reset
    /// the table and the sampling scale, and return the summary.
    ///
    /// Returns `None` when the window recorded no accesses — a window with no
    /// subject has no measurement to report, and a `0/0` emission would be a series
    /// with nothing in it. This is the surface that keeps every correctness test in
    /// this change off the wall clock.
    pub fn close_window(&self) -> Option<WindowSummary> {
        let mut guard = self.write_state();
        let state = guard.as_mut()?;
        let recorded = self.recorded.swap(0, Ordering::Relaxed);
        let dropped = self.dropped.swap(0, Ordering::Relaxed);

        let mut summary = WindowSummary {
            sample_denominator: 1u64 << state.prefix_bits,
            at_sampling_floor: state.at_sampling_floor,
            recorded_accesses: recorded,
            dropped_accesses: dropped,
            ..Default::default()
        };
        state.table.for_each_entry(|e| {
            let idx = RepeatBucket::from_count(e.count).index();
            let b = &mut summary.buckets[idx];
            b.accesses = b.accesses.saturating_add(u64::from(e.count));
            if e.size_unavailable {
                b.distinct_unavailable += 1;
            } else {
                if e.size_from_gap {
                    b.distinct_successor_gap += 1;
                } else {
                    b.distinct_index += 1;
                }
                b.bytes = b.bytes.saturating_add(e.bytes);
            }
        });

        state.table.reset();
        state.prefix_bits = 0;
        state.at_sampling_floor = false;
        state.started = Instant::now();

        if summary.recorded_accesses == 0 {
            return None;
        }
        Some(summary)
    }

    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, Option<WindowState>> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, Option<WindowState>> {
        self.state.write().unwrap_or_else(|e| e.into_inner())
    }
}

/// The process-global recorder behind [`record_partition_access`].
pub fn global() -> &'static PartitionAccessRecorder {
    static GLOBAL: OnceLock<PartitionAccessRecorder> = OnceLock::new();
    GLOBAL.get_or_init(PartitionAccessRecorder::default)
}

/// Record one LOGICAL partition access on the process-global recorder.
///
/// A no-op — one relaxed atomic load, no allocation, no emission — when the probe
/// is disabled, which is the default.
///
/// Call this ONCE per logical partition read, never once per per-SSTable probe:
/// with size-tiered compaction a live partition is present in several generations
/// at once, so per-probe counting would multiply every partition's repeat count by
/// the generation count, shift the whole histogram right and manufacture
/// concentration the workload does not have — a bias toward "build the cache".
#[inline]
pub fn record_partition_access(key: &[u8], weight: AccessWeight) {
    if !enabled() {
        return;
    }
    if let Some(summary) = global().record(key, weight) {
        emit(&summary);
    }
}

/// Close the process-global window deterministically and emit its series.
///
/// Returns the summary, or `None` when the window recorded nothing.
pub fn close_window() -> Option<WindowSummary> {
    let summary = global().close_window()?;
    emit(&summary);
    Some(summary)
}

/// Emit one closed window's four series. Exactly once per closed window; a window
/// with no accesses never reaches here.
fn emit(summary: &WindowSummary) {
    use super::add_counter;
    for b in RepeatBucket::ALL {
        let stats = summary.bucket(b);
        let bucket: AttrValue = b.label().into();
        for (count, source) in [
            (stats.distinct_index, SizeSource::Index),
            (stats.distinct_successor_gap, SizeSource::SuccessorGap),
            (stats.distinct_unavailable, SizeSource::Unavailable),
        ] {
            if count > 0 {
                add_counter(
                    catalog::READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
                    count,
                    &[
                        (catalog::attr::REPEAT_BUCKET, bucket.clone()),
                        (catalog::attr::SIZE_SOURCE, source.label().into()),
                    ],
                );
            }
        }
        if stats.accesses > 0 {
            add_counter(
                catalog::READ_PARTITION_ACCESS_ACCESSES,
                stats.accesses,
                &[(catalog::attr::REPEAT_BUCKET, bucket.clone())],
            );
        }
        if stats.bytes > 0 {
            add_counter(
                catalog::READ_PARTITION_ACCESS_BYTES,
                stats.bytes,
                &[(catalog::attr::REPEAT_BUCKET, bucket)],
            );
        }
    }
    // Saturating rather than wrapping: the denominator is at most 2^20, so this
    // cast is exact, but the clamp keeps the gauge honest under any future cap.
    super::record_gauge(
        catalog::READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR,
        i64::try_from(summary.sample_denominator).unwrap_or(i64::MAX),
        &[],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_boundaries_are_exactly_the_six_declared_ranges() {
        let expected: &[(u32, RepeatBucket)] = &[
            (1, RepeatBucket::One),
            (2, RepeatBucket::Two),
            (3, RepeatBucket::ThreeToFour),
            (4, RepeatBucket::ThreeToFour),
            (5, RepeatBucket::FiveToEight),
            (8, RepeatBucket::FiveToEight),
            (9, RepeatBucket::NineToSixteen),
            (16, RepeatBucket::NineToSixteen),
            (17, RepeatBucket::SeventeenPlus),
            (u32::MAX, RepeatBucket::SeventeenPlus),
        ];
        for (count, want) in expected {
            assert_eq!(RepeatBucket::from_count(*count), *want, "count {count}");
        }
    }

    #[test]
    fn bucket_labels_are_the_six_verbatim_values() {
        let labels: Vec<&str> = RepeatBucket::ALL.iter().map(|b| b.label()).collect();
        assert_eq!(labels, vec!["1", "2", "3-4", "5-8", "9-16", "17+"]);
    }

    #[test]
    fn probe_flag_parsing_rejects_unrecognised_values() {
        assert_eq!(parse_probe_flag("1"), Some(true));
        assert_eq!(parse_probe_flag(" TRUE "), Some(true));
        assert_eq!(parse_probe_flag("on"), Some(true));
        assert_eq!(parse_probe_flag("0"), Some(false));
        assert_eq!(parse_probe_flag("off"), Some(false));
        assert_eq!(parse_probe_flag(""), Some(false));
        assert_eq!(parse_probe_flag("yes please"), None);
        assert_eq!(parse_probe_flag("maybe"), None);
    }

    #[test]
    fn an_unpriced_builder_finishes_unavailable_rather_than_zero_bytes() {
        assert_eq!(
            AccessWeightBuilder::new().finish(),
            AccessWeight::Unavailable,
            "an access with nothing to price is not an access priced at zero"
        );
        let mut b = AccessWeightBuilder::new();
        b.note_sized(100);
        b.note_sized(200);
        assert_eq!(b.finish(), AccessWeight::Index(300));

        // A measured extent is reported with its own provenance, never as `index`.
        let mut b = AccessWeightBuilder::new();
        b.note_measured(4_096);
        assert_eq!(b.finish(), AccessWeight::SuccessorGap(4_096));

        // BTI's `data_size = 0` is not a size; it poisons the access.
        let mut b = AccessWeightBuilder::new();
        b.note_sized(100);
        b.note_sized(0);
        assert_eq!(b.finish(), AccessWeight::Unavailable);

        let mut b = AccessWeightBuilder::new();
        b.note_sized(100);
        b.note_unsized();
        assert_eq!(b.finish(), AccessWeight::Unavailable);
    }

    #[test]
    fn a_disabled_recorder_allocates_nothing() {
        let r = PartitionAccessRecorder::default();
        assert_eq!(r.footprint_bytes(), 0);
        assert_eq!(r.close_window(), None);
        assert_eq!(r.footprint_bytes(), 0);
    }

    #[test]
    fn footprint_is_fixed_once_recording_starts() {
        let r = PartitionAccessRecorder::default();
        r.record(b"k", AccessWeight::SuccessorGap(1));
        assert_eq!(
            r.footprint_bytes(),
            PartitionAccessRecorder::declared_footprint_bytes()
        );
        assert_eq!(r.footprint_bytes(), 3 * 1024 * 1024);
    }
}
