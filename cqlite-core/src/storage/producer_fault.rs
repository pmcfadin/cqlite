//! TEST-ONLY fault injection for the producer-thread boundaries that used to
//! collapse a dead producer into a clean end of input: the query row stream's two
//! (issue #3106) and the k-way MERGE's shared row-forward funnel (issue #3120).
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
//! Two independent seams, one per boundary:
//!
//! * [`arm_query_row_producer_panic`] → the OUTER boundary: the query-row producer
//!   THREAD panics at a batch handoff ([`ProducerFault::before_batch_handoff`],
//!   consulted in `query_rows::emit_rows`).
//! * [`arm_inner_scan_task_panic`] → the INNER boundary: the batched-scan `tokio`
//!   TASK panics ([`inner_scan_task_checkpoint`]). ONE checkpoint site, in that
//!   task's cursor-open prelude
//!   (`data_access::batched_scan_stream::SSTableReader::open_batched_scan_cursor`),
//!   which sits ABOVE the `requires_chunk_stitching()` branch and is therefore
//!   reached whatever on-disk format the reader has — a checkpoint inside either
//!   branch would fire only for that branch's formats and could silently not fire.
//!   This is the arm a `do_get` with NO token filter takes, so it is the issue's
//!   own repro; killing the task drops its sender with no terminator, which the
//!   query-row thread used to read as "the scan finished".
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
#[derive(Debug, Default)]
pub(crate) struct MergeProducerFault {
    /// Rows this producer may still forward into the merge channel before it
    /// panics. `None` = no fault armed (always the case in production).
    #[cfg(any(test, feature = "producer-fault-injection"))]
    panic_after_rows: Option<u64>,
}

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

/// Consulted at the INNER batched-scan task's checkpoint (its cursor-open
/// prelude) so an armed fault unwinds THAT task — the boundary whose disconnect
/// the query-row thread used to read as "the scan finished".
///
/// Panics ON PURPOSE when an arm scoped to THIS reader is registered, taking that
/// arm so exactly one scan task dies. Compiles to an empty function — which never
/// calls `scope_of` — in a production build.
#[inline]
pub(crate) fn inner_scan_task_checkpoint(scope_of: impl FnOnce() -> std::path::PathBuf) {
    #[cfg(not(any(test, feature = "producer-fault-injection")))]
    let _ = scope_of;
    #[cfg(any(test, feature = "producer-fault-injection"))]
    armed::take_inner_and_panic(&scope_of().to_string_lossy());
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

    /// One registered INNER arm.
    struct InnerArm {
        id: u64,
        scope: String,
    }

    /// One registered MERGE arm (issue #3120). Shaped like [`OuterArm`] (it has a
    /// row budget) but in its OWN registry, so a merge producer can never consume
    /// a query-row test's arm — see the module doc.
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
    static INNER_ARMS: Mutex<Vec<InnerArm>> = Mutex::new(Vec::new());
    static MERGE_ARMS: Mutex<Vec<MergeArm>> = Mutex::new(Vec::new());

    /// Distinguishes two arms with the same scope, so a guard removes exactly its
    /// own registration.
    static NEXT_ARM_ID: AtomicU64 = AtomicU64::new(0);

    fn next_id() -> u64 {
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

    /// Fail LOUDLY on a scope that would match everything.
    fn check_scope(scope: &str) {
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

    pub(super) fn disarm_merge(id: u64) {
        MERGE_ARMS.lock().retain(|arm| arm.id != id);
    }

    /// Take the MERGE arm whose scope this run's reader path matches, if any. A
    /// non-matching path leaves every arm registered — which is what stops one
    /// run of a K-input merge from consuming the arm meant for another (a
    /// TempDir-wide scope would kill whichever producer reached its first row
    /// first: a nondeterministic victim AND a nondeterministic rows-through
    /// count).
    pub(super) fn take_merge(path: &str) -> Option<u64> {
        let mut arms = MERGE_ARMS.lock();
        let index = arms
            .iter()
            .position(|arm| path.contains(arm.scope.as_str()))?;
        Some(arms.remove(index).after_rows)
    }

    pub(super) fn arm_inner(scope: &str) -> u64 {
        check_scope(scope);
        let id = next_id();
        INNER_ARMS.lock().push(InnerArm {
            id,
            scope: scope.to_string(),
        });
        id
    }

    pub(super) fn disarm_inner(id: u64) {
        INNER_ARMS.lock().retain(|arm| arm.id != id);
    }

    /// TAKE the inner arm scoped to this reader and panic. Taken (not merely
    /// read) and the lock RELEASED before the panic, so the unwind cannot
    /// re-enter and a retry/sibling scan is never hit by the same arm.
    pub(super) fn take_inner_and_panic(path: &str) {
        let armed = {
            let mut arms = INNER_ARMS.lock();
            match arms
                .iter()
                .position(|arm| path.contains(arm.scope.as_str()))
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
pub fn arm_inner_scan_task_panic(scope: &str) -> ArmedInnerScanPanic {
    ArmedInnerScanPanic {
        id: armed::arm_inner(scope),
    }
}

/// Guard returned by [`arm_inner_scan_task_panic`]: removes its own arm on drop.
/// Holds no lock, so it is safe to hold across an `.await`.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[derive(Debug)]
pub struct ArmedInnerScanPanic {
    id: u64,
}

#[cfg(any(test, feature = "producer-fault-injection"))]
impl Drop for ArmedInnerScanPanic {
    fn drop(&mut self) {
        armed::disarm_inner(self.id);
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
/// TEST-ONLY: this symbol does not exist unless `cfg(test)` or the
/// `producer-fault-injection` feature is on.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[must_use = "the fault stays armed only while the guard is alive"]
pub fn arm_merge_producer_panic(scope: &str, after_rows: u64) -> ArmedMergeProducerPanic {
    ArmedMergeProducerPanic {
        id: armed::arm_merge(scope, after_rows),
    }
}

/// Guard returned by [`arm_merge_producer_panic`]: removes its own arm on drop.
/// Holds no lock, so it is safe to hold across an `.await`.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[derive(Debug)]
pub struct ArmedMergeProducerPanic {
    id: u64,
}

#[cfg(any(test, feature = "producer-fault-injection"))]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn path_of(s: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(s)
    }

    /// A default-constructed fault (the production shape) NEVER panics, however
    /// many batches are handed over.
    #[test]
    fn an_unarmed_producer_fault_is_inert() {
        let mut fault = ProducerFault::default();
        for _ in 0..1000 {
            fault.before_batch_handoff();
        }
    }

    /// The OUTER budget semantics, asserted WITHOUT touching the arm registry
    /// (`for_test`): the fault survives exactly its budget of handoffs, then
    /// panics.
    #[test]
    fn an_armed_producer_fault_panics_after_exactly_its_budget() {
        let mut fault = ProducerFault::for_test(2);
        fault.before_batch_handoff();
        fault.before_batch_handoff();
        // The panic is EXPECTED, so only THIS message is silenced (a blanket hook
        // would hide a real failure from a parallel test) and the previous hook is
        // restored before the assertion runs.
        let died = {
            let _silence = silence_injected_panics();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fault.before_batch_handoff();
            }))
        };
        assert!(
            died.is_err(),
            "the armed fault must panic on the handoff after its budget"
        );
    }

    /// SCOPING is what makes the registry safe in a shared test binary (roborev,
    /// issue #3106): a reader whose path does not match must NOT take the arm, and
    /// must leave it registered for the reader it was meant for.
    #[test]
    fn an_arm_is_taken_only_by_a_matching_reader_path() {
        let scope = "issue-3106-scoping-unit-test/only-me";
        let guard = arm_query_row_producer_panic(scope, 5);

        // A foreign reader (what a concurrently-running sibling test looks like)
        // captures NOTHING and leaves the arm in place.
        for foreign in [
            "/tmp/other-test/data/ks/tbl/nb-1-big-Data.db",
            "/tmp/issue-3106-scoping-unit-test/someone-else/nb-1-big-Data.db",
        ] {
            let stolen = ProducerFault::capture_for(|| path_of(foreign));
            assert_eq!(
                stolen.panic_after_batches, None,
                "a non-matching reader path must not consume the arm ({foreign})"
            );
        }

        // The intended reader takes it, exactly once.
        let matching = format!("/tmp/{scope}/nb-1-big-Data.db");
        assert_eq!(
            ProducerFault::capture_for(|| path_of(&matching)).panic_after_batches,
            Some(5),
            "the reader the arm was scoped to takes it"
        );
        assert_eq!(
            ProducerFault::capture_for(|| path_of(&matching)).panic_after_batches,
            None,
            "and it is consumed — a second stream over the same reader is clean"
        );
        drop(guard);
    }

    /// Two arms coexist: arming second does not clobber the first, and each guard
    /// removes only its OWN registration. This is the property a single global
    /// slot could not provide, and it is why parallel arming tests are safe.
    #[test]
    fn concurrent_arms_coexist_and_each_guard_removes_only_its_own() {
        let first = arm_query_row_producer_panic("issue-3106-coexist/alpha", 1);
        let second = arm_query_row_producer_panic("issue-3106-coexist/beta", 2);

        drop(first);
        assert_eq!(
            ProducerFault::capture_for(|| path_of("/x/issue-3106-coexist/alpha/d.db"))
                .panic_after_batches,
            None,
            "dropping the first guard removes ONLY its arm"
        );
        assert_eq!(
            ProducerFault::capture_for(|| path_of("/x/issue-3106-coexist/beta/d.db"))
                .panic_after_batches,
            Some(2),
            "the second arm survives its sibling's disarm, un-clobbered"
        );
        drop(second);
    }

    /// A default-constructed MERGE fault (the production shape) NEVER panics,
    /// however many rows are forwarded.
    #[test]
    fn an_unarmed_merge_producer_fault_is_inert() {
        let mut fault = MergeProducerFault::default();
        for _ in 0..1000 {
            fault.before_row_forward();
        }
    }

    /// The MERGE budget semantics, asserted WITHOUT touching the arm registry
    /// (`for_test`): the fault survives exactly its budget of row forwards, then
    /// panics.
    #[test]
    fn an_armed_merge_producer_fault_panics_after_exactly_its_budget() {
        let mut fault = MergeProducerFault::for_test(2);
        fault.before_row_forward();
        fault.before_row_forward();
        let died = {
            let _silence = silence_injected_panics();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                fault.before_row_forward();
            }))
        };
        assert!(
            died.is_err(),
            "the armed merge fault must panic on the row forward after its budget"
        );
    }

    /// The MERGE registry is SEPARATE from the query-row one (issue #3120): a
    /// merge run must not consume a query-row arm, and a query row stream must not
    /// consume a merge arm — cross-consumption would let either test pass for the
    /// wrong reason.
    #[test]
    fn merge_and_query_row_registries_never_consume_each_others_arms() {
        let scope = "issue-3120-registry-separation/only-me";
        let path = format!("/tmp/{scope}/nb-1-big-Data.db");

        // A MERGE arm is invisible to the query-row capture...
        let merge_guard = arm_merge_producer_panic(scope, 3);
        assert_eq!(
            ProducerFault::capture_for(|| path_of(&path)).panic_after_batches,
            None,
            "a query row stream must not consume a MERGE arm"
        );
        assert_eq!(
            MergeProducerFault::capture_for(|| path_of(&path)).panic_after_rows,
            Some(3),
            "the merge run the arm was scoped to takes it"
        );
        drop(merge_guard);

        // ...and symmetrically, a query-row arm is invisible to the merge capture.
        let query_guard = arm_query_row_producer_panic(scope, 4);
        assert_eq!(
            MergeProducerFault::capture_for(|| path_of(&path)).panic_after_rows,
            None,
            "a merge run must not consume a QUERY-ROW arm"
        );
        assert_eq!(
            ProducerFault::capture_for(|| path_of(&path)).panic_after_batches,
            Some(4),
            "the query row stream the arm was scoped to takes it"
        );
        drop(query_guard);
    }

    /// A MERGE arm is taken ONLY by a matching input path — the property that
    /// makes a K-input merge's victim deterministic (a `TempDir`-wide scope would
    /// kill whichever producer reached its first row first).
    #[test]
    fn a_merge_arm_is_taken_only_by_a_matching_input_path() {
        let scope = "issue-3120-merge-scope/nb-2-big-Data.db";
        let guard = arm_merge_producer_panic(scope, 1);

        let sibling = MergeProducerFault::capture_for(|| {
            path_of("/tmp/issue-3120-merge-scope/nb-1-big-Data.db")
        });
        assert_eq!(
            sibling.panic_after_rows, None,
            "the SIBLING input of the same merge must not consume the arm"
        );
        assert_eq!(
            MergeProducerFault::capture_for(|| path_of(&format!("/tmp/{scope}")))
                .panic_after_rows,
            Some(1),
            "the input the arm was scoped to takes it"
        );
        assert_eq!(
            MergeProducerFault::capture_for(|| path_of(&format!("/tmp/{scope}")))
                .panic_after_rows,
            None,
            "and it is consumed — a retry over the same input is clean"
        );
        drop(guard);
    }

    /// The INNER checkpoint obeys the same scoping rule: a foreign scan passes
    /// through untouched, the scoped one dies, and the arm is consumed.
    #[test]
    fn the_inner_checkpoint_fires_only_for_the_scoped_reader() {
        let scope = "issue-3106-inner-scope-unit-test/only-me";
        let guard = arm_inner_scan_task_panic(scope);
        let matching = format!("/tmp/{scope}/nb-1-big-Data.db");

        // A foreign scan must run right through the checkpoint.
        inner_scan_task_checkpoint(|| path_of("/tmp/someone-else/nb-1-big-Data.db"));

        let died = {
            let _silence = silence_injected_panics();
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                inner_scan_task_checkpoint(|| path_of(&matching));
            }))
        };
        assert!(died.is_err(), "the scoped scan must hit the injected panic");

        // Consumed: a retry of the same scan is clean.
        inner_scan_task_checkpoint(|| path_of(&matching));
        drop(guard);
    }
}
