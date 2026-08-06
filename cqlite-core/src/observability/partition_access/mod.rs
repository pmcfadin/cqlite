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

mod config;
pub mod decision;
mod table;
mod types;

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use super::{catalog, AttrValue};
use table::{Insert, Table};

pub use config::{
    enabled, parse_probe_flag, set_probe_enabled, window_config_from_env, WindowConfig,
    DEFAULT_MAX_PREFIX_BITS,
};
pub use decision::{Refusal, Verdict};
pub use types::{
    AccessWeight, AccessWeightBuilder, BucketStats, RepeatBucket, SizeSource, WindowSummary,
};

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
    /// Accesses banked against THIS window. Lives in the state — not beside it —
    /// so a `close_window` can never bank an access in one generation while its
    /// table entry lands in the next (C3).
    recorded: AtomicU64,
    /// Accesses this window could not seat in its table at all (C4/B8).
    dropped: AtomicU64,
    table: Table,
    prefix_bits: u32,
    at_sampling_floor: bool,
    started: Instant,
}

/// A bounded partition repeat-access recorder.
///
/// Owned instances exist so tests can drive an entire window with no
/// process-global state and no cross-test interference; the process-global
/// instance behind [`record_partition_access`] is one of these.
pub struct PartitionAccessRecorder {
    state: RwLock<Option<WindowState>>,
    /// Monotonic window-close counter. Bumped under the WRITE lock inside
    /// [`Self::close_locked`], so a summary's sequence orders it against every other
    /// close on this recorder even though the emit happens after the lock drops.
    close_sequence: AtomicU64,
    /// Highest `close_sequence + 1` whose per-window gauges have been published, for
    /// THIS recorder. Guarded by a mutex rather than an atomic because the compare
    /// and the three gauge writes must be ONE critical section — see
    /// [`Self::emit_window`].
    gauge_order: std::sync::Mutex<u64>,
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
            close_sequence: AtomicU64::new(0),
            gauge_order: std::sync::Mutex::new(0),
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
    /// `scope` names the table the access belongs to and `key` is the raw
    /// partition-key bytes the read path already holds. Both are hashed together
    /// for slot addressing and within-window identity; neither is stored, logged,
    /// or emitted. The scope is part of the identity because ONE recorder serves
    /// every table — see [`table::hash_partition`].
    ///
    /// Returns the summaries of any windows this access CLOSED, so the global
    /// wrapper can emit them. Usually empty. Both triggers can fire for one access —
    /// the duration bound before it is seated and the access-count bound after — so
    /// this can carry two, and neither may be discarded.
    pub fn record(
        &self,
        scope: TableScope<'_>,
        key: &[u8],
        weight: AccessWeight,
    ) -> Vec<WindowSummary> {
        let hash = table::hash_partition(scope.keyspace, scope.table, key);
        let bytes = weight.bytes();
        let flags = match weight.source() {
            SizeSource::SuccessorGap => table::FLAG_SIZE_FROM_GAP,
            SizeSource::Index | SizeSource::Unavailable => 0,
        };

        // Close BEFORE seating this access, not after (E2).
        //
        // The window is TUMBLING, so an access arriving past the boundary belongs to
        // the NEXT window. Seating it first and closing afterwards banked it in the
        // window it had already left: a key touched at t=0 and again at t=61s
        // reported a repeat count of 2 inside one nominal 60 s window, which
        // OVERSTATES concentration. The design accepts the opposite bias — a
        // partition split across a boundary becomes two lower-repeat entries, so the
        // histogram understates — precisely because understating is the safe
        // direction for a go/no-go.
        let closed = self.close_if_expired();
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
                    // Landed. Bank it against THIS state, under the same read lock
                    // that received the entry (C3): `close_window` consumes the pair
                    // under the WRITE lock, so a close can never bank the access in
                    // one generation while its entry sits in the next.
                    Insert::Recorded => {
                        state.recorded.fetch_add(1, Ordering::Relaxed);
                        if state.table.occupancy() >= table::LOAD_FACTOR_LIMIT {
                            slow = SlowPath::Downsample { replay: false };
                        }
                    }
                    // The sampling predicate declined the key. Still an access this
                    // window was asked to record, and banked here for the same
                    // reason.
                    Insert::NotAdmitted => {
                        state.recorded.fetch_add(1, Ordering::Relaxed);
                    }
                    // NOT banked here: the write-locked path both seats and banks it
                    // against whichever generation it ends up holding. Banking it now
                    // would reintroduce the split a concurrent close causes.
                    Insert::Full => slow = SlowPath::Downsample { replay: true },
                }
            } else {
                // No table yet — the write-locked path allocates, seats and banks.
                slow = SlowPath::Allocate;
            }
        }

        match slow {
            SlowPath::None => {}
            SlowPath::Allocate => self.grow_or_downsample(hash, bytes, flags, true),
            SlowPath::Downsample { replay } => self.grow_or_downsample(hash, bytes, flags, replay),
        }
        // A count-bound close is evaluated AFTER seating: the bound is on accesses
        // RECORDED, so the access that reaches it belongs to the window it filled.
        //
        // BOTH can fire in one `record` — reachable at
        // `CQLITE_PARTITION_ACCESS_WINDOW_ACCESSES=1`, where every access both
        // expires the previous window and immediately fills its own. Returning only
        // one of the two summaries would DROP a closed window's measurement on the
        // floor, so the duration-close is emitted here and the count-close is
        // returned to the caller.
        // Returning `Option` would DROP one of the two on the floor. Emitting the
        // extra one here instead is not an option either: an OWNED recorder (every
        // recorder-level test) must stay free of process-global emission, which is
        // the whole reason `record` returns summaries rather than emitting them.
        // A `Vec` does not allocate while empty, so the common path is unchanged.
        let mut closed_windows = Vec::new();
        closed_windows.extend(closed);
        closed_windows.extend(self.close_if_over_count());
        closed_windows
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
                    recorded: AtomicU64::new(0),
                    dropped: AtomicU64::new(0),
                    table: Table::new(),
                    prefix_bits: 0,
                    at_sampling_floor: false,
                    started: Instant::now(),
                };
                if replay {
                    state.table.record_with_flags(hash, 0, bytes, flags);
                    state.recorded.fetch_add(1, Ordering::Relaxed);
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
                // it, banking it against THIS state (C3).
                //
                // C4: a `Full` can happen well BELOW the load factor — the probe
                // bound is 64 slots and, as occupancy approaches 0.75, the expected
                // longest cluster is several hundred, so a new key whose home sits
                // in a long run finds no slot while the widen loop above (gated on
                // occupancy) never fires. Dropping there would be biased in the
                // dangerous direction: an EXISTING entry is always within 64 probes
                // of its home, so only NEW keys are lost, which suppresses the
                // singleton bucket and OVERSTATES concentration — exactly the "makes
                // the cache look good" bias D4 rejects eviction for. So widen (an
                // unbiased, frequency-independent thinning) and retry until the key
                // is seated or the prefix cap is reached.
                if replay {
                    let mut seated =
                        state
                            .table
                            .record_with_flags(hash, state.prefix_bits, bytes, flags);
                    while seated == Insert::Full
                        && state.prefix_bits < self.max_prefix_bits.load(Ordering::Relaxed)
                    {
                        state.prefix_bits += 1;
                        state.table.downsample(state.prefix_bits);
                        state.at_sampling_floor |=
                            state.prefix_bits >= self.max_prefix_bits.load(Ordering::Relaxed);
                        seated =
                            state
                                .table
                                .record_with_flags(hash, state.prefix_bits, bytes, flags);
                    }
                    // Banked either way: it is an access this window was asked to
                    // record. `Full` at the cap is input LOST and is additionally
                    // reported as dropped, so no reader has to infer it.
                    state.recorded.fetch_add(1, Ordering::Relaxed);
                    if seated == Insert::Full {
                        state.dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Close the window if its wall-clock length has elapsed. Called BEFORE an
    /// access is seated, so the access that crosses the boundary opens the NEXT
    /// window instead of being folded into the one it just left (E2).
    ///
    /// This also bounds an idle process: a window whose duration elapsed while
    /// nothing was being recorded is closed by the next access rather than silently
    /// spanning the idle period. A window with no accesses at all still emits
    /// nothing — there is no measurement to report.
    fn close_if_expired(&self) -> Option<WindowSummary> {
        let duration_nanos = self.duration_nanos.load(Ordering::Relaxed);
        self.close_window_if(|state| {
            u64::try_from(state.started.elapsed().as_nanos()).unwrap_or(u64::MAX) >= duration_nanos
        })
    }

    /// Close the window if it has recorded its access bound. Evaluated AFTER the
    /// access is seated: the bound counts accesses RECORDED, so the one that reaches
    /// it belongs to the window it completed.
    fn close_if_over_count(&self) -> Option<WindowSummary> {
        let max_accesses = self.max_accesses.load(Ordering::Relaxed);
        self.close_window_if(|state| state.recorded.load(Ordering::Relaxed) >= max_accesses)
    }

    /// Close the window IFF `should_close` holds — evaluated under the SAME write
    /// lock that performs the close.
    ///
    /// # Why the predicate cannot be checked separately
    ///
    /// Both triggers used to read their predicate under the READ lock, drop the
    /// guard, then call [`Self::close_window`], which takes the write lock and closes
    /// whatever window it finds — re-checking nothing. `record_partition_access` runs
    /// on concurrent read tasks, so two threads could both observe "expired" or "over
    /// count" and both close: the first emitted the real window and reset it
    /// (`table.reset()`, `prefix_bits = 0`, `started = Instant::now()`), and the
    /// second then closed the FRESH one. If accesses had landed in the gap that
    /// emitted a spurious micro-window of near-all singletons; if none had, it still
    /// reset `started` and shifted the next boundary.
    ///
    /// The bias direction is the safe one — splitting repeats across two windows
    /// lowers `H_max` — but it corrupts the per-window series an operator prices from,
    /// which is the whole point of the boundary semantics. Nothing in the approved
    /// spec or design sanctions concurrent closes, so it is a gap, not a behaviour.
    ///
    /// Distinct from the seat-then-close ordering fixed earlier (E2): that was
    /// single-threaded ordering, this is check-then-act across two lock acquisitions.
    fn close_window_if(
        &self,
        should_close: impl FnOnce(&WindowState) -> bool,
    ) -> Option<WindowSummary> {
        let mut guard = self.write_state();
        if !should_close(guard.as_ref()?) {
            return None;
        }
        self.close_locked(&mut guard)
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
        self.close_locked(&mut guard)
    }

    /// The close itself, with the write lock already held. Single implementation so
    /// a conditional close cannot diverge from an unconditional one.
    ///
    /// Stamps [`WindowSummary::close_sequence`] here, under the lock, so the ORDER of
    /// closes is fixed even though emission happens after the lock is released.
    fn close_locked(&self, guard: &mut Option<WindowState>) -> Option<WindowSummary> {
        let state = guard.as_mut()?;
        let recorded = state.recorded.swap(0, Ordering::Relaxed);
        let dropped = state.dropped.swap(0, Ordering::Relaxed);

        let mut summary = WindowSummary {
            close_sequence: self.close_sequence.fetch_add(1, Ordering::Relaxed),
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

    /// Emit one closed window's series, sequencing the per-window GAUGES.
    ///
    /// The counter families are additive and emitted unconditionally. The three
    /// unlabelled gauges are last-writer-wins and carry the normative "was the LAST
    /// CLOSED window clean" property, so a stale window must not write them.
    ///
    /// Two properties this needs, both of which an earlier `fetch_max` high-water
    /// mark failed to provide:
    ///
    /// - **Per-recorder scope.** `close_sequence` is per-recorder, so one
    ///   PROCESS-GLOBAL mark conflated independent recorders: an owned recorder
    ///   emitting sequences 0,1 and the global one emitting 0,1,2 would suppress each
    ///   other's gauges depending on interleaving — silently, since a suppressed
    ///   gauge is an ABSENT series and `counter_sum` reads absent as `0.0`.
    /// - **Atomic compare-and-write.** `fetch_max` decides WHETHER an emitter may
    ///   write; it does not serialize the writes. Older (seq 5) could be admitted,
    ///   be preempted, let newer (seq 6) admit and write, then resume and overwrite
    ///   with stale values — inverting exactly the property the sequencing exists to
    ///   guarantee. Both callers emit outside the recorder's window lock by design,
    ///   so that interleaving is reachable.
    ///
    /// Hence one mutex per recorder held across the comparison AND all three writes.
    /// It is taken once per CLOSED WINDOW, never on the record hot path.
    fn emit_window(&self, summary: &WindowSummary) {
        emit_counters(summary);

        let mut high_water = self
            .gauge_order
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // `high_water` is the highest sequence already published, PLUS ONE, so a
        // sequence strictly below it has been superseded.
        if summary.close_sequence < *high_water {
            // A newer window of THIS recorder already published; skip, never queue.
            return;
        }
        *high_water = summary.close_sequence + 1;

        // Saturating rather than wrapping: the denominator is at most 2^20, so the
        // cast is exact, but the clamp keeps the gauge honest under any future cap.
        super::record_gauge(
            catalog::READ_PARTITION_ACCESS_SAMPLE_DENOMINATOR,
            i64::try_from(summary.sample_denominator).unwrap_or(i64::MAX),
            &[],
        );
        // The two per-window trustworthiness gauges: a window is clean exactly when
        // both read 0, and both are emitted on EVERY closed window including at zero
        // so an absent series is never ambiguous.
        super::record_gauge(
            catalog::READ_PARTITION_ACCESS_WINDOW_DROPPED,
            i64::try_from(summary.dropped_accesses).unwrap_or(i64::MAX),
            &[],
        );
        super::record_gauge(
            catalog::READ_PARTITION_ACCESS_SAMPLING_FLOOR,
            i64::from(summary.at_sampling_floor),
            &[],
        );
    }

    /// [`Self::emit_window`], for tests that need to control emission ORDER.
    ///
    /// The sequencing guarantee is only observable when two windows are emitted out
    /// of order or concurrently, which cannot be produced deterministically by the
    /// ordinary paths. Gated to `observability-testing`, the feature that installs the
    /// in-memory meter, so it is absent from every production build. Being a METHOD
    /// is load-bearing: the ordering state it exercises is per-recorder.
    #[cfg(feature = "observability-testing")]
    pub fn emit_for_test(&self, summary: &WindowSummary) {
        self.emit_window(summary);
    }

    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, Option<WindowState>> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, Option<WindowState>> {
        self.state.write().unwrap_or_else(|e| e.into_inner())
    }
}

/// The process-global recorder behind [`record_partition_access`].
///
/// Initialised from [`window_config_from_env`], so the window length and access
/// bound are operator-reachable without a code change; [`PartitionAccessRecorder::set_window_config`]
/// still overrides at runtime.
pub fn global() -> &'static PartitionAccessRecorder {
    static GLOBAL: OnceLock<PartitionAccessRecorder> = OnceLock::new();
    GLOBAL.get_or_init(|| PartitionAccessRecorder::new(window_config_from_env()))
}

/// The table an access belongs to — part of the entry identity.
///
/// A borrowed pair rather than an owned/formatted name so the hot path allocates
/// nothing. Both call sites already hold this and previously discarded it, which is
/// how the same key in two tables came to share one entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableScope<'a> {
    /// Keyspace name, or `""` when the caller has only a bare table name.
    pub keyspace: &'a str,
    /// Table name.
    pub table: &'a str,
}

impl<'a> TableScope<'a> {
    /// A scope from an explicit keyspace and table.
    pub fn new(keyspace: &'a str, table: &'a str) -> Self {
        Self { keyspace, table }
    }

    /// A scope from a possibly-qualified `keyspace.table` identifier, as
    /// [`crate::types::TableId`] carries. An unqualified name yields an empty
    /// keyspace, which is still a distinct scope from any qualified one.
    pub fn from_qualified(name: &'a str) -> Self {
        match name.split_once('.') {
            Some((ks, table)) => Self {
                keyspace: ks,
                table,
            },
            None => Self {
                keyspace: "",
                table: name,
            },
        }
    }
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
pub fn record_partition_access(scope: TableScope<'_>, key: &[u8], weight: AccessWeight) {
    if !enabled() {
        return;
    }
    for summary in global().record(scope, key, weight) {
        global().emit_window(&summary);
    }
}

/// Close the process-global window deterministically and emit its series.
///
/// Returns the summary, or `None` when the window recorded nothing.
pub fn close_window() -> Option<WindowSummary> {
    let summary = global().close_window()?;
    global().emit_window(&summary);
    Some(summary)
}

/// Emit the ORDER-FREE part of a closed window: the three bucketed counter families
/// and the cumulative drop counter. Additive, so no sequencing is needed.
fn emit_counters(summary: &WindowSummary) {
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
    // The CUMULATIVE drop counter is additive — "has this process ever lost input" —
    // so it is emitted for every closed window regardless of ordering.
    if summary.dropped_accesses > 0 {
        add_counter(
            catalog::READ_PARTITION_ACCESS_DROPPED,
            summary.dropped_accesses,
            &[],
        );
    }
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
        r.record(
            TableScope::new("ks", "t"),
            b"k",
            AccessWeight::SuccessorGap(1),
        );
        assert_eq!(
            r.footprint_bytes(),
            PartitionAccessRecorder::declared_footprint_bytes()
        );
        assert_eq!(r.footprint_bytes(), 3 * 1024 * 1024);
    }
}
