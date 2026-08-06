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

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use super::{catalog, AttrValue};
use table::{Insert, Table};

pub use decision::{Refusal, Verdict};

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

/// The six repeat-access buckets, verbatim as specified. A closed set — the whole
/// cardinality budget of the emitted series.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RepeatBucket {
    /// Accessed exactly once in the window.
    One,
    /// Accessed exactly twice.
    Two,
    /// Accessed 3 or 4 times.
    ThreeToFour,
    /// Accessed 5–8 times.
    FiveToEight,
    /// Accessed 9–16 times.
    NineToSixteen,
    /// Accessed 17 or more times.
    SeventeenPlus,
}

impl RepeatBucket {
    /// Every bucket, in ascending order.
    pub const ALL: [RepeatBucket; 6] = [
        RepeatBucket::One,
        RepeatBucket::Two,
        RepeatBucket::ThreeToFour,
        RepeatBucket::FiveToEight,
        RepeatBucket::NineToSixteen,
        RepeatBucket::SeventeenPlus,
    ];

    /// The bounded attribute value for `cqlite.read.repeat_bucket`.
    pub fn label(self) -> &'static str {
        match self {
            RepeatBucket::One => "1",
            RepeatBucket::Two => "2",
            RepeatBucket::ThreeToFour => "3-4",
            RepeatBucket::FiveToEight => "5-8",
            RepeatBucket::NineToSixteen => "9-16",
            RepeatBucket::SeventeenPlus => "17+",
        }
    }

    /// Classify a repeat count. `0` is not a valid input (an entry exists only
    /// because it was accessed at least once) and is classified as [`Self::One`].
    pub fn from_count(count: u32) -> Self {
        match count {
            0 | 1 => RepeatBucket::One,
            2 => RepeatBucket::Two,
            3..=4 => RepeatBucket::ThreeToFour,
            5..=8 => RepeatBucket::FiveToEight,
            9..=16 => RepeatBucket::NineToSixteen,
            _ => RepeatBucket::SeventeenPlus,
        }
    }

    fn index(self) -> usize {
        match self {
            RepeatBucket::One => 0,
            RepeatBucket::Two => 1,
            RepeatBucket::ThreeToFour => 2,
            RepeatBucket::FiveToEight => 3,
            RepeatBucket::NineToSixteen => 4,
            RepeatBucket::SeventeenPlus => 5,
        }
    }
}

/// Provenance of an access's on-disk byte weight — the closed
/// `cqlite.read.size_source` value set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeSource {
    /// Every SSTable resolved for the access reported an authoritative size.
    Index,
    /// At least one resolved SSTable reported no authoritative size.
    Unavailable,
}

impl SizeSource {
    /// The bounded attribute value for `cqlite.read.size_source`.
    pub fn label(self) -> &'static str {
        match self {
            SizeSource::Index => "index",
            SizeSource::Unavailable => "unavailable",
        }
    }
}

/// The on-disk byte weight of one logical partition access.
///
/// **`Unavailable` fails closed and is never filled in.** BTI trie resolution
/// records only an offset (`PartitionLoc::offset_only`, `data_size = 0`), so a
/// BTI-resolved access has no authoritative size. Such an access is still COUNTED
/// as a partition access — dropping it would make the histogram itself wrong — but
/// it contributes ZERO bytes and is reported under
/// `distinct_partitions{cqlite.read.size_source="unavailable"}`, so an incomplete
/// byte total always has a visible `unavailable` series beside it and the decision
/// procedure can refuse the window. A size is never estimated, interpolated from a
/// successor offset, or defaulted to a nominal value (no-heuristics, #28).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessWeight {
    /// Authoritative on-disk bytes, summed across the SSTables resolved for this
    /// one logical access.
    Index(u64),
    /// No authoritative size was available for at least one resolved SSTable.
    Unavailable,
}

impl AccessWeight {
    fn bytes(self) -> Option<u64> {
        match self {
            AccessWeight::Index(b) => Some(b),
            AccessWeight::Unavailable => None,
        }
    }
}

/// Accumulates the byte weight of ONE logical partition access across the SSTables
/// that access resolved.
///
/// Fails closed in both directions: an accumulator that saw no authoritative size
/// at all finishes as [`AccessWeight::Unavailable`] (an access with nothing to
/// price is not an access priced at zero), and a single unsized SSTable poisons the
/// whole access.
#[derive(Clone, Copy, Debug, Default)]
pub struct AccessWeightBuilder {
    bytes: u64,
    sized: u32,
    unavailable: bool,
}

impl AccessWeightBuilder {
    /// A fresh accumulator for one logical access.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that one resolved SSTable reported `data_size` on-disk bytes for this
    /// partition. A `data_size` of `0` is NOT a size — it is how BTI resolution
    /// records "the trie knows the offset and nothing else" — so it is folded in as
    /// [`Self::note_unsized`].
    pub fn note_sized(&mut self, data_size: u32) {
        if data_size == 0 {
            self.note_unsized();
            return;
        }
        self.bytes = self.bytes.saturating_add(u64::from(data_size));
        self.sized = self.sized.saturating_add(1);
    }

    /// Record that one resolved SSTable reported no authoritative size.
    pub fn note_unsized(&mut self) {
        self.unavailable = true;
    }

    /// Finish the accumulation.
    pub fn finish(self) -> AccessWeight {
        if self.unavailable || self.sized == 0 {
            AccessWeight::Unavailable
        } else {
            AccessWeight::Index(self.bytes)
        }
    }
}

/// Per-bucket totals for one closed window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BucketStats {
    /// Distinct partitions in this bucket whose bytes were authoritative.
    pub distinct_index: u64,
    /// Distinct partitions in this bucket whose bytes could not be priced.
    pub distinct_unavailable: u64,
    /// Sum of the repeat counts of every partition in this bucket.
    pub accesses: u64,
    /// Sum of DISTINCT-partition on-disk bytes in this bucket (unavailable
    /// partitions contribute zero, by construction).
    pub bytes: u64,
}

impl BucketStats {
    /// Distinct partitions in this bucket, priced or not.
    pub fn distinct(&self) -> u64 {
        self.distinct_index + self.distinct_unavailable
    }
}

/// The summary of one CLOSED measurement window — the complete input to the
/// decision procedure.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WindowSummary {
    buckets: [BucketStats; 6],
    /// `2^k` for the sampling prefix width `k` in force at close. `1` = census.
    pub sample_denominator: u64,
    /// The recorder hit its sampling floor: the surviving sample is too small to
    /// be worth anything and the decision procedure refuses the window.
    pub at_sampling_floor: bool,
    /// Every access the recorder was asked to record, including accesses to keys
    /// the sampling predicate did not admit. Always `>= total_accesses()`.
    pub recorded_accesses: u64,
}

impl WindowSummary {
    /// Stats for one bucket.
    pub fn bucket(&self, b: RepeatBucket) -> BucketStats {
        self.buckets[b.index()]
    }

    /// `A = Σ a_b` — accesses attributable to the admitted sample.
    pub fn total_accesses(&self) -> u64 {
        self.buckets.iter().map(|b| b.accesses).sum()
    }

    /// Distinct partitions in the admitted sample.
    pub fn distinct_partitions(&self) -> u64 {
        self.buckets.iter().map(|b| b.distinct()).sum()
    }

    /// Distinct partitions whose on-disk bytes could not be priced.
    pub fn unavailable_partitions(&self) -> u64 {
        self.buckets.iter().map(|b| b.distinct_unavailable).sum()
    }

    /// Total distinct-partition on-disk bytes across every bucket.
    pub fn total_bytes(&self) -> u64 {
        self.buckets.iter().map(|b| b.bytes).sum()
    }

    /// A census window counted every distinct partition it saw.
    pub fn is_census(&self) -> bool {
        self.sample_denominator == 1
    }

    /// Fraction of distinct partitions whose bytes could not be priced. `0.0` for
    /// an empty window.
    pub fn unavailable_fraction(&self) -> f64 {
        let total = self.distinct_partitions();
        if total == 0 {
            return 0.0;
        }
        self.unavailable_partitions() as f64 / total as f64
    }
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
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(60),
            max_accesses: 5_000_000,
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

/// Sampling-prefix cap. At `k = 20` the sample is 1-in-1,048,576, which over a
/// field-scale corpus admits a couple of keys — statistically worthless, so the
/// window is marked non-census and the decision procedure refuses it.
const MAX_PREFIX_BITS: u32 = 20;

/// A bounded partition repeat-access recorder.
///
/// Owned instances exist so tests can drive an entire window with no
/// process-global state and no cross-test interference; the process-global
/// instance behind [`record_partition_access`] is one of these.
pub struct PartitionAccessRecorder {
    state: RwLock<Option<WindowState>>,
    recorded: AtomicU64,
    config: WindowConfig,
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
        Self {
            state: RwLock::new(None),
            recorded: AtomicU64::new(0),
            config,
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
        self.recorded.fetch_add(1, Ordering::Relaxed);

        // Fast path: shared read lock, relaxed atomics on one slot.
        let mut slow = SlowPath::None;
        {
            let guard = self.read_state();
            if let Some(state) = guard.as_ref() {
                match state.table.record(hash, state.prefix_bits, bytes) {
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
            SlowPath::Allocate => self.grow_or_downsample(hash, bytes, true),
            SlowPath::Downsample { replay } => self.grow_or_downsample(hash, bytes, replay),
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
    fn grow_or_downsample(&self, hash: u64, bytes: Option<u64>, replay: bool) {
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
                    state.table.record(hash, 0, bytes);
                }
                *guard = Some(state);
            }
            Some(state) => {
                // Widen until the table is back under its load factor, or until the
                // sampling floor is reached (at which point the window is marked
                // non-census and simply stops admitting new keys — survivors keep
                // counting so the window stays internally consistent).
                while state.table.occupancy() >= table::LOAD_FACTOR_LIMIT
                    && state.prefix_bits < MAX_PREFIX_BITS
                {
                    state.prefix_bits += 1;
                    state.table.downsample(state.prefix_bits);
                }
                if state.prefix_bits >= MAX_PREFIX_BITS
                    && state.table.occupancy() >= table::LOAD_FACTOR_LIMIT
                {
                    state.at_sampling_floor = true;
                }
                // Re-attempt the access ONLY if the fast path did not already
                // count it. A `Full` here (an unlucky 64-long probe run at the
                // floor) simply drops the access rather than looping.
                if replay {
                    let _ = state.table.record(hash, state.prefix_bits, bytes);
                }
            }
        }
    }

    fn close_if_triggered(&self) -> Option<WindowSummary> {
        let (elapsed, over_count) = {
            let guard = self.read_state();
            let state = guard.as_ref()?;
            (
                state.started.elapsed() >= self.config.duration,
                self.recorded.load(Ordering::Relaxed) >= self.config.max_accesses,
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

        let mut summary = WindowSummary {
            sample_denominator: 1u64 << state.prefix_bits,
            at_sampling_floor: state.at_sampling_floor,
            recorded_accesses: recorded,
            ..Default::default()
        };
        state.table.for_each_entry(|e| {
            let idx = RepeatBucket::from_count(e.count).index();
            let b = &mut summary.buckets[idx];
            b.accesses = b.accesses.saturating_add(u64::from(e.count));
            if e.size_unavailable {
                b.distinct_unavailable += 1;
            } else {
                b.distinct_index += 1;
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
        if stats.distinct_index > 0 {
            add_counter(
                catalog::READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
                stats.distinct_index,
                &[
                    (catalog::attr::REPEAT_BUCKET, bucket.clone()),
                    (catalog::attr::SIZE_SOURCE, SizeSource::Index.label().into()),
                ],
            );
        }
        if stats.distinct_unavailable > 0 {
            add_counter(
                catalog::READ_PARTITION_ACCESS_DISTINCT_PARTITIONS,
                stats.distinct_unavailable,
                &[
                    (catalog::attr::REPEAT_BUCKET, bucket.clone()),
                    (
                        catalog::attr::SIZE_SOURCE,
                        SizeSource::Unavailable.label().into(),
                    ),
                ],
            );
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
        r.record(b"k", AccessWeight::Index(1));
        assert_eq!(
            r.footprint_bytes(),
            PartitionAccessRecorder::declared_footprint_bytes()
        );
        assert_eq!(r.footprint_bytes(), 3 * 1024 * 1024);
    }
}
