//! TEST-ONLY fault injection for the producer boundaries that used to collapse a
//! dead producer into a clean end of input: the query row stream's two (issue
//! #3106), the k-way MERGE's shared row-forward funnel (issue #3120), and every
//! spawned task on the multi-generation streaming-scan path (issue #3124).
//!
//! # Why this exists
//!
//! The query row stream ([`crate::storage::sstable::reader::QueryRowStream`]) is
//! fed by a detached producer thread over a bounded channel, and on the full-ring
//! arm that thread is in turn fed by an INNER `tokio` task over a second channel.
//! Before issue #3106 BOTH boundaries collapsed a channel DISCONNECT into a clean
//! end of stream, so a producer that UNWOUND (a panic anywhere in the walk/decode,
//! rather than an `Err` return) dropped its sender without a terminal message and
//! the request completed SUCCESSFULLY with a silently truncated result set. The
//! fix makes completion explicit at both boundaries; this module is how a test
//! PROVES that, deterministically, without waiting for a real decode bug to
//! unwind.
//!
//! Independent seams, one per boundary:
//!
//! * [`arm_query_row_producer_panic`] → the OUTER boundary: the query-row producer
//!   THREAD panics at a batch handoff ([`ProducerFault::before_batch_handoff`],
//!   consulted in `query_rows::emit_rows`).
//! * [`arm_inner_scan_task_panic`] → the INNER boundary: the batched-scan `tokio`
//!   TASK panics ([`inner_scan_task_checkpoint`]). ONE checkpoint site, in that
//!   task's cursor-open prelude
//!   (`data_access::joined_scan_stream::SSTableReader::open_batched_scan_cursor`),
//!   which sits ABOVE the `requires_chunk_stitching()` branch and is therefore
//!   reached whatever on-disk format the reader has — a checkpoint inside either
//!   branch would fire only for that branch's formats and could silently not fire.
//!   This is the arm a `do_get` with NO token filter takes, so it is the issue's
//!   own repro; killing the task drops its sender with no terminator, which the
//!   query-row thread used to read as "the scan finished".
//! * [`arm_scan_task_panic`] → the generalisation of the second seam to EVERY
//!   spawned scan task on the ≠1-generation (query-engine full scan) path (issue
//!   #3124): the fan-out k-way merge task, a per-reader per-row sub-scan, the
//!   windowed forwarder, and the cross-generation RECONCILING merge task — see
//!   [`ScanTaskSite`]. Each had a DISCARDED `JoinHandle`
//!   and a consumer that read channel-close as end-of-scan, i.e. the #3106 defect
//!   on the multi-generation path. An arm is keyed by `(site, scope)`, not scope
//!   alone, because one scan traverses several of these checkpoints.
//! * [`arm_merge_producer_panic`] → the K-WAY MERGE boundary (issue #3120): a
//!   merge producer THREAD panics at a row forward
//!   ([`MergeProducerFault::before_row_forward`], consulted in
//!   `write_engine::merge::from_readers::forward_row`). That is the `emit`
//!   callback BOTH `stream_all_partitions_for_compaction` and
//!   `..._for_query` invoke, and it sits in `write_engine/merge` ABOVE any
//!   reader format branch — so unlike a checkpoint inside the reader there is no
//!   `requires_chunk_stitching()`-style bypass that could make the fault silently
//!   not fire, and ONE funnel covers BOTH producer shapes (path-based compaction
//!   and shared-reader warm query).
//!
//!   Its registry is SEPARATE from the query-row one ([`arm_merge_producer_panic`]
//!   vs [`arm_query_row_producer_panic`]) on purpose: a shared registry would let
//!   a merge producer consume an arm a query-row test registered (and vice versa),
//!   which is exactly the cross-consumption class the per-reader scoping below
//!   exists to eliminate.
//!
//! # Every arm is SCOPED to one reader — no test can take another's (roborev)
//!
//! An arm is registered against a `scope` STRING, and a checkpoint fires only when
//! the scanning reader's `Data.db` PATH contains that scope. This is the
//! structural replacement for the earlier "process-global, caller serializes"
//! convention, which was unsound in the `cqlite-core` lib test binary: the in-`src`
//! panic tests compile into the SAME binary as thousands of other tests and
//! libtest runs them in parallel, so a concurrent test's scan could consume the arm
//! and let a panic-injection test pass for the wrong reason (a doc comment asking
//! callers to serialize is exactly the mitigation that failed).
//!
//! Two properties make it structural rather than conventional:
//!
//! * **Scoped consumption.** A scan whose path does not match leaves the arm
//!   registered, so it cannot consume a foreign arm even by racing. A test scopes
//!   to its own `TempDir` path (unique per run) or to its own `keyspace/table`.
//! * **A registry, not a slot.** Arms live in a `Vec`, so two concurrently armed
//!   tests coexist; neither can clobber the other's arm by arming second. Each
//!   guard removes its OWN entry (by id) on drop.
//!
//! Matching is by substring so a caller can scope to a directory (`keyspace/table`)
//! without knowing the generated SSTable filename. An arm that matches nothing is
//! simply never taken — the arming test then fails LOUDLY (it sees no error), which
//! is the correct failure direction.
//!
//! # Why this is not a production knob (no-heuristics safe)
//!
//! * Everything that can arm a fault is `#[cfg(any(test, feature =
//!   "producer-fault-injection"))]`. In a default build the arming API does not
//!   exist, the registries do not exist, [`ProducerFault`] is a zero-sized struct
//!   with no fields, and both checkpoints compile to empty functions that never
//!   even evaluate their scope closure — so a production scan does not pay the
//!   `PathBuf` clone. The production build is byte-identical to one without this
//!   module's body, and the module itself is `pub(crate)` there.
//! * No environment variable, config field or on-disk byte pattern can arm it: the
//!   only way in is a Rust call to an arming function that does not exist in
//!   production builds. So it cannot influence a decoding decision (issue #28)
//!   even accidentally.
//! * `cqlite-flight` enables the feature from its `[dev-dependencies]` (the same
//!   convention `arrow-shape-corpus` / `test-util` already use), so the shipped
//!   Flight binary never links it.
//!
//! # One seam injects an `Err`, not a panic (issue #3154)
//!
//! [`arm_merge_construction_error`] (child module [`construction`]) makes
//! `KWayMerger::new` REPORT a chosen error variant on the cross-generation merge
//! path, which is what proves the narrowed fallback classification: an I/O or
//! corruption failure must propagate, while a merger-ineligible unsupported-format
//! failure must still degrade to the documented concat. See that module's doc.

/// The `Err`-reporting construction seam (issue #3154), in a child module so this
/// file stays under the ~800-line campsite target (epic #1116).
///
/// Gated to exactly where the seam it injures EXISTS —
/// `generation_merge::stream_generations_for_read` is `write-support` AND
/// `not(tombstones)` (a `tombstones` build routes `scan_stream` through the
/// materializing `scan`, which has no `MergeStreamSetupError` at all) — for the same
/// reason [`ScanTaskSite::CrossGenerationMerge`] is: a symbol that nothing in a
/// configuration can ever reach would be a lie about that configuration's coverage,
/// and `#[allow(dead_code)]` would hide it instead of stating it.
#[cfg(all(
    any(test, feature = "producer-fault-injection"),
    feature = "write-support",
    not(feature = "tombstones")
))]
mod construction;
#[cfg(all(
    any(test, feature = "producer-fault-injection"),
    feature = "write-support",
    not(feature = "tombstones")
))]
pub use construction::{
    arm_merge_construction_error, ArmedMergeConstructionError, MergeConstructionFault,
    INJECTED_CONSTRUCTION_MESSAGE,
};

/// Producer-fault state captured ONCE when a query row stream is opened, then
/// owned by that stream's producer thread.
///
/// A zero-sized, no-op struct in a production build (see the module doc).
#[derive(Debug, Default)]
pub(crate) struct ProducerFault {
    /// Batches this producer may still hand to the consumer before it panics.
    /// `None` = no fault armed (always the case in production).
    #[cfg(any(test, feature = "producer-fault-injection"))]
    panic_after_batches: Option<u64>,
}

impl ProducerFault {
    /// Take the OUTER arm registered for this reader, if any (never any, in
    /// production — `scope_of` is not even called).
    ///
    /// `scope_of` is lazy precisely so the production build pays nothing: it
    /// clones a `PathBuf` only when the arming surface is compiled in.
    pub(crate) fn capture_for(scope_of: impl FnOnce() -> std::path::PathBuf) -> Self {
        #[cfg(not(any(test, feature = "producer-fault-injection")))]
        let _ = scope_of;
        Self {
            #[cfg(any(test, feature = "producer-fault-injection"))]
            panic_after_batches: armed::take_outer(&scope_of().to_string_lossy()),
        }
    }

    /// Consulted by the producer THREAD immediately BEFORE it hands a batch to
    /// the consumer — the single batch-handoff funnel both walk arms go through,
    /// so an injected fault is observed identically on either.
    ///
    /// Panics ON PURPOSE once the armed batch budget is exhausted; that panic is
    /// the fault being injected, and it is what the producer's `catch_unwind`
    /// must convert into a terminal error instead of a silent truncation.
    /// Compiles to an empty function in a production build.
    pub(crate) fn before_batch_handoff(&mut self) {
        #[cfg(any(test, feature = "producer-fault-injection"))]
        if let Some(remaining) = self.panic_after_batches.as_mut() {
            if *remaining == 0 {
                panic!("{}", INJECTED_PANIC_MESSAGE);
            }
            *remaining -= 1;
        }
    }

    /// Build an OUTER fault state directly, WITHOUT touching the arm registry —
    /// so a unit test of this module's own logic needs no scope and can never
    /// race, or be raced by, a sibling test.
    #[cfg(test)]
    fn for_test(panic_after_batches: u64) -> Self {
        Self {
            panic_after_batches: Some(panic_after_batches),
        }
    }
}

/// Merge-producer fault state captured ONCE when a k-way merge run's producer
/// thread starts, then owned by that thread (issue #3120).
///
/// A zero-sized, no-op struct in a production build, exactly like
/// [`ProducerFault`] (see the module doc).
///
/// Gated on `write-support` — unlike its query-row sibling [`ProducerFault`],
/// which serves a READ-path stream that exists unconditionally. The k-way merge,
/// its producer threads, and the `forward_row` funnel this checkpoint lives in are
/// ALL `#[cfg(feature = "write-support")]` (`storage::write_engine::merge`), so
/// without that feature there is no producer thread to injure and this type has no
/// possible constructor. The gate's `minimal-build`
/// (`--no-default-features --features all-compression`) proves it: an ungated
/// version is genuinely dead code there, which is the wiring telling the truth
/// rather than a warning to silence.
#[cfg(feature = "write-support")]
#[derive(Debug, Default)]
pub(crate) struct MergeProducerFault {
    /// Rows this producer may still forward into the merge channel before it
    /// panics. `None` = no fault armed (always the case in production).
    #[cfg(any(test, feature = "producer-fault-injection"))]
    panic_after_rows: Option<u64>,
}

#[cfg(feature = "write-support")]
impl MergeProducerFault {
    /// Take the MERGE arm registered for this run's reader, if any (never any, in
    /// production — `scope_of` is not even called).
    pub(crate) fn capture_for(scope_of: impl FnOnce() -> std::path::PathBuf) -> Self {
        #[cfg(not(any(test, feature = "producer-fault-injection")))]
        let _ = scope_of;
        Self {
            #[cfg(any(test, feature = "producer-fault-injection"))]
            panic_after_rows: armed::take_merge(&scope_of().to_string_lossy()),
        }
    }

    /// Consulted by a merge producer THREAD immediately BEFORE it forwards one
    /// converted row into the bounded merge channel — the single emit funnel both
    /// the compaction stream and the warm query stream go through, so an injected
    /// fault is observed identically on either.
    ///
    /// Panics ON PURPOSE once the armed row budget is exhausted; that panic is the
    /// fault being injected, and it is what the producer's `catch_unwind` must
    /// convert into a terminal `MergeMsg::Failed` instead of a dropped sender the
    /// merge reads as "this run is exhausted". Compiles to an empty function in a
    /// production build.
    pub(crate) fn before_row_forward(&mut self) {
        #[cfg(any(test, feature = "producer-fault-injection"))]
        if let Some(remaining) = self.panic_after_rows.as_mut() {
            if *remaining == 0 {
                panic!("{}", INJECTED_PANIC_MESSAGE);
            }
            *remaining -= 1;
        }
    }

    /// Build a MERGE fault state directly, WITHOUT touching the arm registry — so
    /// a unit test of this module's own logic needs no scope and can never race,
    /// or be raced by, a sibling test.
    #[cfg(test)]
    fn for_test(panic_after_rows: u64) -> Self {
        Self {
            panic_after_rows: Some(panic_after_rows),
        }
    }
}

/// Which spawned scan task a checkpoint belongs to (issue #3124).
///
/// Part of an arm's key, not just documentation: the four #3124 boundaries sit on
/// ONE code path, so a single fan-out scan runs through the merge task, each
/// per-reader sub-scan and (on a compressed reader) the windowed forwarder. Keyed by
/// scope ALONE, a test arming the boundary it means to prove would have its arm
/// consumed by whichever checkpoint the scan reached first, and would then pass
/// while the boundary under test was never exercised.
///
/// Exists in production builds too (it is a checkpoint PARAMETER), where every
/// checkpoint compiles to an empty function that never inspects it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanTaskSite {
    /// The BATCHED scan's driver task, at its cursor-open prelude
    /// (`open_batched_scan_cursor`) — the issue-#3106 inner boundary.
    InnerBatchedScan,
    /// A PER-ROW `scan_stream_admitted` sub-scan task, at its prelude (issue #3124
    /// site 2). Each generation of a fan-out merge runs one of these.
    PerRowScan,
    /// The fan-out k-way MERGE task of `SSTableManager::scan_stream` (issue #3124
    /// site 1) — the multi-generation path's top-level producer.
    ///
    /// Constructed only on the non-`tombstones` path: a `tombstones` build routes
    /// `scan_stream` through the materializing `scan`, so there is no fan-out merge
    /// task there and this variant is legitimately unconstructed in that config.
    #[cfg_attr(feature = "tombstones", allow(dead_code))]
    FanoutMerge,
    /// The windowed scan's FORWARDER task (issue #3124 site 4), which adapts the
    /// blocking parse half's batches to the caller's surface. Reached only by
    /// chunk-stitching (compressed) readers.
    WindowedForwarder,
    /// The CROSS-GENERATION reconciling merge task of
    /// `generation_merge::stream_generations_for_read` (issue #3124 site 5), at its
    /// prelude — i.e. inside the `KWayMerger::new` construction window, BEFORE the
    /// task signals readiness.
    ///
    /// Deliberately its own site rather than sharing [`Self::FanoutMerge`]: that is
    /// the schema-less lazy token-order CONCAT, this is the authoritative RECONCILING
    /// merge a multi-generation read with a schema takes, and the whole point of the
    /// site is that a death here must NOT be answered by silently substituting the
    /// concat (roborev on #3124).
    ///
    /// # Gated to exactly where the site EXISTS
    ///
    /// `generation_merge` is a `write-support` module (it drives the write engine's
    /// `KWayMerger`) and its streaming entry point is additionally
    /// `cfg(not(tombstones))` — a `tombstones` build routes `scan_stream` through the
    /// materializing `scan`. So this checkpoint exists in exactly one configuration,
    /// and the variant is `#[cfg]`'d to match it rather than kept everywhere behind an
    /// `allow(dead_code)`: in a read-only (`--no-default-features`) build there is no
    /// cross-generation merge task to arm, and a variant nothing can ever construct
    /// there would be a lie about the seam's coverage. Its only two references — the
    /// checkpoint call and the end-to-end pin in
    /// `scan_stream_fanout_panic_tests.rs` — carry this same triple already.
    ///
    /// ([`Self::FanoutMerge`] cannot use a `#[cfg]` here: this module's own always-
    /// compiled unit tests name it, so under `--all-features` (i.e. `tombstones` on)
    /// cfg'ing it away would break those tests rather than silence a lint.)
    #[cfg(all(feature = "write-support", not(feature = "tombstones")))]
    CrossGenerationMerge,
}

/// Consulted at a spawned scan task's checkpoint so an armed fault unwinds THAT
/// task — reproducing exactly the condition every one of these boundaries used to
/// read as "the scan finished": the task's sender drops with no error and no
/// terminator.
///
/// Panics ON PURPOSE when an arm for this SITE, scoped to THIS reader, is
/// registered, taking that arm so exactly one task dies. Compiles to an empty
/// function — which never calls `scope_of` — in a production build.
#[inline]
pub(crate) fn scan_task_checkpoint(
    site: ScanTaskSite,
    scope_of: impl FnOnce() -> std::path::PathBuf,
) {
    #[cfg(not(any(test, feature = "producer-fault-injection")))]
    {
        let _ = site;
        let _ = scope_of;
    }
    #[cfg(any(test, feature = "producer-fault-injection"))]
    armed::take_task_and_panic(site, &scope_of().to_string_lossy());
}

/// The INNER batched-scan task's checkpoint (its cursor-open prelude) — the
/// boundary whose disconnect the query-row thread used to read as "the scan
/// finished" (issue #3106). A named wrapper over
/// [`scan_task_checkpoint`] so the #3106 call site reads as its own boundary.
#[inline]
pub(crate) fn inner_scan_task_checkpoint(scope_of: impl FnOnce() -> std::path::PathBuf) {
    scan_task_checkpoint(ScanTaskSite::InnerBatchedScan, scope_of)
}

/// A checkpoint scope CAPTURED for a task that will run later, elsewhere (issue
/// #3124).
///
/// [`scan_task_checkpoint`] takes a lazy closure because its callers hold the reader
/// right there. Two #3124 sites do not: the fan-out merge task and the windowed
/// forwarder are `tokio::spawn`ed with an environment that must OWN whatever they
/// check, and the forwarder is spawned from a function with no reader in scope. This
/// type is that owned scope — and it is a ZERO-SIZED, no-op struct in a production
/// build, so a production scan clones no `PathBuf` and the spawned task's
/// environment grows by nothing.
#[derive(Debug, Clone, Default)]
pub(crate) struct FaultScope {
    #[cfg(any(test, feature = "producer-fault-injection"))]
    path: std::path::PathBuf,
}

impl FaultScope {
    /// Capture the scope a later-spawned task will check against. `of` is called
    /// only when the arming surface is compiled in.
    pub(crate) fn capture(of: impl FnOnce() -> std::path::PathBuf) -> Self {
        #[cfg(not(any(test, feature = "producer-fault-injection")))]
        let _ = of;
        Self {
            #[cfg(any(test, feature = "producer-fault-injection"))]
            path: of(),
        }
    }

    /// The captured scope's checkpoint for `site`. Empty function in production.
    #[inline]
    pub(crate) fn checkpoint(&self, site: ScanTaskSite) {
        #[cfg(not(any(test, feature = "producer-fault-injection")))]
        let _ = site;
        #[cfg(any(test, feature = "producer-fault-injection"))]
        armed::take_task_and_panic(site, &self.path.to_string_lossy());
    }
}

/// The `Err`-reporting construction seam's take side (issue #3154).
///
/// Gated to the one configuration whose call site exists — the cross-generation
/// reconciling merge's construction window in
/// `generation_merge::stream_generations_for_read` — so no build carries a method
/// nothing can reach (see [`construction`]'s `mod` declaration above).
#[cfg(all(feature = "write-support", not(feature = "tombstones")))]
impl FaultScope {
    /// The construction error a test armed for this scope, if any — i.e. the error
    /// `KWayMerger::new` is made to report instead of building the merger.
    ///
    /// ALWAYS `None` in a production build, where [`Self::armed_construction_error`]
    /// is a function that returns `None` without touching (or even compiling) any
    /// registry.
    #[inline]
    pub(crate) fn injected_construction_error(&self) -> Option<crate::Error> {
        self.armed_construction_error()
    }

    #[cfg(any(test, feature = "producer-fault-injection"))]
    #[inline]
    fn armed_construction_error(&self) -> Option<crate::Error> {
        construction::take(&self.path.to_string_lossy())
    }

    /// Production build: nothing can be armed, so there is nothing to take.
    #[cfg(not(any(test, feature = "producer-fault-injection")))]
    #[inline]
    fn armed_construction_error(&self) -> Option<crate::Error> {
        None
    }
}

/// The panic message both checkpoints raise. Exported so a test can (a) assert
/// the forwarded error carries it and (b) suppress exactly this panic in its
/// panic hook without silencing a real one.
#[cfg(any(test, feature = "producer-fault-injection"))]
pub const INJECTED_PANIC_MESSAGE: &str =
    "cqlite test fault injection (issue #3106): producer panic";

#[cfg(any(test, feature = "producer-fault-injection"))]
mod armed {
    use std::sync::atomic::{AtomicU64, Ordering};

    use parking_lot::Mutex;

    /// One registered OUTER arm.
    struct OuterArm {
        id: u64,
        scope: String,
        after_batches: u64,
    }

    /// One registered TASK arm: a scope PLUS the checkpoint site it applies to
    /// (issue #3124). The site is part of the key, so arming the fan-out merge over
    /// a fixture cannot be consumed by that same fixture's per-reader sub-scan — the
    /// four #3124 sites sit on ONE code path and every scan runs through several of
    /// them, so a scope-only key would let a test prove the wrong boundary.
    struct TaskArm {
        id: u64,
        scope: String,
        site: super::ScanTaskSite,
    }

    /// One registered MERGE arm (issue #3120). Shaped like [`OuterArm`] (it has a
    /// row budget) but in its OWN registry, so a merge producer can never consume
    /// a query-row test's arm — see the module doc.
    ///
    /// `write-support`-gated for the same reason [`super::MergeProducerFault`] is:
    /// with no k-way merge compiled there is no producer thread to arm, so the
    /// whole MERGE registry would be dead.
    #[cfg(feature = "write-support")]
    struct MergeArm {
        id: u64,
        scope: String,
        after_rows: u64,
    }

    /// Registered arms. A `Vec`, not a slot: concurrently armed tests coexist and
    /// cannot clobber one another (see the module doc). Every critical section
    /// below is a few comparisons long and NEVER spans an `.await`, so no guard is
    /// ever held across a suspension point.
    static OUTER_ARMS: Mutex<Vec<OuterArm>> = Mutex::new(Vec::new());
    static TASK_ARMS: Mutex<Vec<TaskArm>> = Mutex::new(Vec::new());
    #[cfg(feature = "write-support")]
    static MERGE_ARMS: Mutex<Vec<MergeArm>> = Mutex::new(Vec::new());

    /// Distinguishes two arms with the same scope, so a guard removes exactly its
    /// own registration.
    static NEXT_ARM_ID: AtomicU64 = AtomicU64::new(0);

    /// `pub(super)` so the sibling [`construction`](super::construction) registry
    /// shares ONE id space with these three, rather than minting a second counter
    /// whose ids could collide with theirs in a debugger or a log.
    pub(super) fn next_id() -> u64 {
        NEXT_ARM_ID.fetch_add(1, Ordering::SeqCst)
    }

    /// Shortest scope an arm may register with.
    ///
    /// The match is a substring test, so an empty (or near-empty) scope matches
    /// EVERY reader path and silently restores the process-global behaviour the
    /// scoping exists to eliminate — invisibly, which is the worst failure
    /// direction. Enforced rather than documented, since "callers are careful" is
    /// exactly the mitigation that already failed once here (roborev, #3106). Any
    /// real scope — a `TempDir` path or a `keyspace/table` pair — clears this by an
    /// order of magnitude.
    const MIN_SCOPE_LEN: usize = 8;

    /// Fail LOUDLY on a scope that would match everything. `pub(super)` so every
    /// registry — including the sibling [`construction`](super::construction) one —
    /// enforces the SAME minimum rather than re-deriving it.
    pub(super) fn check_scope(scope: &str) {
        debug_assert!(
            scope.len() >= MIN_SCOPE_LEN,
            "producer-fault scope {scope:?} is shorter than {MIN_SCOPE_LEN} chars: a \
             substring that loose matches every reader path, which silently reverts \
             the arm to process-global (issue #3106) — scope to a TempDir path or a \
             keyspace/table instead"
        );
    }

    pub(super) fn arm_outer(scope: &str, after_batches: u64) -> u64 {
        check_scope(scope);
        let id = next_id();
        OUTER_ARMS.lock().push(OuterArm {
            id,
            scope: scope.to_string(),
            after_batches,
        });
        id
    }

    pub(super) fn disarm_outer(id: u64) {
        OUTER_ARMS.lock().retain(|arm| arm.id != id);
    }

    /// Take the arm whose scope this reader path matches, if any. A non-matching
    /// path leaves every arm registered — which is what makes a foreign scan
    /// unable to consume someone else's arm.
    pub(super) fn take_outer(path: &str) -> Option<u64> {
        let mut arms = OUTER_ARMS.lock();
        let index = arms
            .iter()
            .position(|arm| path.contains(arm.scope.as_str()))?;
        Some(arms.remove(index).after_batches)
    }

    #[cfg(feature = "write-support")]
    pub(super) fn arm_merge(scope: &str, after_rows: u64) -> u64 {
        check_scope(scope);
        let id = next_id();
        MERGE_ARMS.lock().push(MergeArm {
            id,
            scope: scope.to_string(),
            after_rows,
        });
        id
    }

    #[cfg(feature = "write-support")]
    pub(super) fn disarm_merge(id: u64) {
        MERGE_ARMS.lock().retain(|arm| arm.id != id);
    }

    /// Take the MERGE arm whose scope this run's reader path matches, if any. A
    /// non-matching path leaves every arm registered — which is what stops one
    /// run of a K-input merge from consuming the arm meant for another (a
    /// TempDir-wide scope would kill whichever producer reached its first row
    /// first: a nondeterministic victim AND a nondeterministic rows-through
    /// count).
    #[cfg(feature = "write-support")]
    pub(super) fn take_merge(path: &str) -> Option<u64> {
        let mut arms = MERGE_ARMS.lock();
        let index = arms
            .iter()
            .position(|arm| path.contains(arm.scope.as_str()))?;
        Some(arms.remove(index).after_rows)
    }

    pub(super) fn arm_task(scope: &str, site: super::ScanTaskSite) -> u64 {
        check_scope(scope);
        let id = next_id();
        TASK_ARMS.lock().push(TaskArm {
            id,
            scope: scope.to_string(),
            site,
        });
        id
    }

    pub(super) fn disarm_task(id: u64) {
        TASK_ARMS.lock().retain(|arm| arm.id != id);
    }

    /// TAKE the arm registered for THIS site and scoped to this reader, then panic.
    /// Taken (not merely read) and the lock RELEASED before the panic, so the unwind
    /// cannot re-enter and a retry/sibling scan is never hit by the same arm.
    ///
    /// Both parts of the key must match: an arm registered for another site is left
    /// registered even when the path matches (issue #3124), so a checkpoint upstream
    /// of the one under test cannot consume it.
    pub(super) fn take_task_and_panic(site: super::ScanTaskSite, path: &str) {
        let armed = {
            let mut arms = TASK_ARMS.lock();
            match arms
                .iter()
                .position(|arm| arm.site == site && path.contains(arm.scope.as_str()))
            {
                Some(index) => {
                    arms.remove(index);
                    true
                }
                None => false,
            }
        };
        if armed {
            panic!("{}", super::INJECTED_PANIC_MESSAGE);
        }
    }
}

/// Arm the next query row stream opened over a reader whose `Data.db` path
/// contains `scope` to panic in its producer THREAD just before it hands over
/// batch number `after_batches` (0-based), so `after_batches` batches reach the
/// consumer and the walk then dies MID-STREAM.
///
/// `0` kills the producer before its first handoff. Disarmed when the returned
/// guard drops, and TAKEN by the first MATCHING stream — a stream over any other
/// reader leaves it alone, so a concurrently-running test can neither consume nor
/// clobber this arm (see the module doc). Scope to something unique: a test's own
/// `TempDir` path, or `keyspace/table`.
///
/// TEST-ONLY: this symbol does not exist unless `cfg(test)` or the
/// `producer-fault-injection` feature is on.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[must_use = "the fault stays armed only while the guard is alive"]
pub fn arm_query_row_producer_panic(scope: &str, after_batches: u64) -> ArmedProducerPanic {
    ArmedProducerPanic {
        id: armed::arm_outer(scope, after_batches),
    }
}

/// Guard returned by [`arm_query_row_producer_panic`]: removes its own arm on
/// drop. Holds no lock, so it is safe to hold across an `.await`.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[derive(Debug)]
pub struct ArmedProducerPanic {
    id: u64,
}

#[cfg(any(test, feature = "producer-fault-injection"))]
impl Drop for ArmedProducerPanic {
    fn drop(&mut self) {
        armed::disarm_outer(self.id);
    }
}

/// Arm the next batched-scan task over a reader whose `Data.db` path contains
/// `scope` to panic in its cursor-open prelude, so the task unwinds and drops its
/// sender with no error and no terminator.
///
/// It dies before any row and independently of which format branch the reader
/// would have taken; the join that must catch it wraps the whole task, so the
/// property this proves holds for a panic anywhere inside it. There is
/// deliberately no "die after N units" knob — the prelude is the only checkpoint,
/// so a count would be unspendable.
///
/// Disarmed when the returned guard drops, and TAKEN by the first MATCHING scan;
/// a scan over any other reader leaves it registered (see the module doc).
///
/// TEST-ONLY: this symbol does not exist unless `cfg(test)` or the
/// `producer-fault-injection` feature is on.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[must_use = "the fault stays armed only while the guard is alive"]
pub fn arm_inner_scan_task_panic(scope: &str) -> ArmedScanTaskPanic {
    arm_scan_task_panic(scope, ScanTaskSite::InnerBatchedScan)
}

/// Arm the next scan task of `site`, over a reader whose `Data.db` path contains
/// `scope`, to panic at that site's checkpoint — so the task unwinds and drops its
/// sender with no error and no terminator (issue #3124).
///
/// The task dies before any row it would have produced at that site, and the join
/// that must catch it wraps the whole task, so the property proven holds for a panic
/// anywhere inside it. There is deliberately no "die after N units" knob: each site
/// has exactly ONE checkpoint, so a count would be unspendable.
///
/// Disarmed when the returned guard drops, and TAKEN by the first scan that matches
/// BOTH the site and the scope; any other scan — or the same scan at a different
/// site — leaves it registered (see the module doc).
///
/// TEST-ONLY: this symbol does not exist unless `cfg(test)` or the
/// `producer-fault-injection` feature is on.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[must_use = "the fault stays armed only while the guard is alive"]
pub fn arm_scan_task_panic(scope: &str, site: ScanTaskSite) -> ArmedScanTaskPanic {
    ArmedScanTaskPanic {
        id: armed::arm_task(scope, site),
    }
}

/// Guard returned by [`arm_scan_task_panic`] / [`arm_inner_scan_task_panic`]:
/// removes its own arm on drop. Holds no lock, so it is safe to hold across an
/// `.await`.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[derive(Debug)]
pub struct ArmedScanTaskPanic {
    id: u64,
}

#[cfg(any(test, feature = "producer-fault-injection"))]
impl Drop for ArmedScanTaskPanic {
    fn drop(&mut self) {
        armed::disarm_task(self.id);
    }
}

/// Arm the next k-way MERGE run over a reader whose `Data.db` path contains
/// `scope` to panic in its producer THREAD just before it forwards row number
/// `after_rows` (0-based), so `after_rows` rows reach the merge and the run then
/// dies MID-WALK (issue #3120).
///
/// `0` kills the producer before its first row. Disarmed when the returned guard
/// drops, and TAKEN by the first MATCHING run — a run over any other input leaves
/// it alone, so in a K-input merge exactly the intended input's producer dies and
/// the rows-through count is deterministic. Scope to ONE input's `Data.db` path,
/// NOT the enclosing `TempDir` (see [`armed::take_merge`]).
///
/// TEST-ONLY: this symbol does not exist unless (`cfg(test)` or the
/// `producer-fault-injection` feature) AND `write-support` — the k-way merge it
/// injures is itself `write-support`-gated (see [`MergeProducerFault`]).
#[cfg(all(
    feature = "write-support",
    any(test, feature = "producer-fault-injection")
))]
#[must_use = "the fault stays armed only while the guard is alive"]
pub fn arm_merge_producer_panic(scope: &str, after_rows: u64) -> ArmedMergeProducerPanic {
    ArmedMergeProducerPanic {
        id: armed::arm_merge(scope, after_rows),
    }
}

/// Guard returned by [`arm_merge_producer_panic`]: removes its own arm on drop.
/// Holds no lock, so it is safe to hold across an `.await`.
#[cfg(all(
    feature = "write-support",
    any(test, feature = "producer-fault-injection")
))]
#[derive(Debug)]
pub struct ArmedMergeProducerPanic {
    id: u64,
}

#[cfg(all(
    feature = "write-support",
    any(test, feature = "producer-fault-injection")
))]
impl Drop for ArmedMergeProducerPanic {
    fn drop(&mut self) {
        armed::disarm_merge(self.id);
    }
}

/// A boxed panic hook, as [`std::panic::set_hook`] takes it.
#[cfg(any(test, feature = "producer-fault-injection"))]
type PanicHook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send + 'static>;

/// Suppress the console noise of the INJECTED panic — and only that one — for the
/// returned guard's lifetime.
///
/// Deliberately NOT a blanket `set_hook(|_| {})`: the panic hook is
/// process-global, so silencing everything would swallow a genuine assertion
/// failure message from this test or any test running in parallel, exactly the
/// "masked assertion" failure mode. Panics whose payload does not carry
/// [`INJECTED_PANIC_MESSAGE`] are delegated to the hook that was installed
/// before (libtest's capture hook, normally), and that hook is reinstated when
/// this guard drops — EXCEPT on an unwinding drop, where restoring is skipped
/// (see [`SilencedInjectedPanics::drop`]).
#[cfg(any(test, feature = "producer-fault-injection"))]
#[must_use = "the injected panic is only silenced while the guard is alive"]
pub fn silence_injected_panics() -> SilencedInjectedPanics {
    let previous: std::sync::Arc<PanicHook> = std::sync::Arc::new(std::panic::take_hook());
    let installed = previous.clone();
    std::panic::set_hook(Box::new(move |info| {
        if is_injected(info) {
            return;
        }
        installed(info);
    }));
    SilencedInjectedPanics { previous }
}

/// Whether `info` describes an injected fault panic. Matched on the payload
/// STRING, so no real panic is ever swallowed.
#[cfg(any(test, feature = "producer-fault-injection"))]
fn is_injected(info: &std::panic::PanicHookInfo<'_>) -> bool {
    let payload = info.payload();
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&'static str>().copied());
    message.is_some_and(|m| m.contains(INJECTED_PANIC_MESSAGE))
}

/// Guard returned by [`silence_injected_panics`]; restores the previous hook on a
/// normal drop (see [`Self::drop`] for the unwinding case).
#[cfg(any(test, feature = "producer-fault-injection"))]
pub struct SilencedInjectedPanics {
    previous: std::sync::Arc<PanicHook>,
}

#[cfg(any(test, feature = "producer-fault-injection"))]
impl Drop for SilencedInjectedPanics {
    fn drop(&mut self) {
        // NEVER touch the hook while unwinding (roborev, issue #3106):
        // `std::panic::set_hook` PANICS if called from a panicking thread, so a
        // guard still alive when an assertion (or any fallible call inside the
        // silenced block) fails would double-panic and ABORT the process — under
        // libtest's capture that loses the original message entirely and, in an
        // integration-test binary, takes every sibling test's result with it.
        // Skipping the restore leaves the filtering hook installed for the rest of
        // the process, which is harmless: it only ever suppresses
        // `INJECTED_PANIC_MESSAGE` and delegates everything else.
        if std::thread::panicking() {
            return;
        }
        // `set_hook` needs an owned `Box`, and the previous hook is behind an
        // `Arc` (it is borrowed by the filtering hook installed above), so it is
        // reinstated through one thin delegating wrapper. Behaviourally identical;
        // the only cost is a pointer hop per nested guard.
        let previous = self.previous.clone();
        std::panic::set_hook(Box::new(move |info| previous(info)));
    }
}

// Unit tests in a `*_tests.rs` sibling so this file stays under the ~800-line
// campsite target (epic #1116 / #1135) — see that file's header.
#[cfg(test)]
#[path = "producer_fault_tests.rs"]
mod tests;
