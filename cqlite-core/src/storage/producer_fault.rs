//! TEST-ONLY fault injection for the query row stream's two producer boundaries
//! (issue #3106).
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
//!   TASK panics ([`inner_scan_task_checkpoint`]). Two checkpoint sites, both in
//!   `data_access::batched_scan_stream` and both INSIDE that task: its cursor-open
//!   prelude (`open_batched_scan_cursor`, checkpoint 0 — reached whatever on-disk
//!   format the reader has) and each block decode of the non-stitching branch
//!   (`parse_batched_block`, i.e. the `parse_block_entries_at_now` call itself).
//!   This is the arm a `do_get` with NO token filter takes, so it is the issue's
//!   own repro; killing the task there drops its sender with no terminator, which
//!   the query-row thread used to read as "the scan finished".
//!
//! # Why this is not a production knob (no-heuristics safe)
//!
//! * Everything that can arm a fault is `#[cfg(any(test, feature =
//!   "producer-fault-injection"))]`. In a default build the arming API does not
//!   exist, the armed-state statics do not exist, [`ProducerFault`] is a
//!   zero-sized struct with no fields, and both checkpoints compile to empty
//!   functions — the production build is byte-identical to one without this
//!   module's body. The module itself is `pub(crate)`; only the test-only surface
//!   is re-exported, so a default build publishes nothing from here.
//! * No environment variable, config field or on-disk byte pattern can arm it: the
//!   only way in is a Rust call to an arming function that does not exist in
//!   production builds. So it cannot influence a decoding decision (issue #28)
//!   even accidentally.
//! * `cqlite-flight` enables the feature from its `[dev-dependencies]` (the same
//!   convention `arrow-shape-corpus` / `test-util` already use), so the shipped
//!   Flight binary never links it.
//!
//! # Arming semantics — PROCESS-GLOBAL, caller serializes
//!
//! An arm is process-global rather than thread-local on purpose: the Flight
//! `do_get` row route opens its stream on a `spawn_blocking` thread and drives the
//! inner scan on yet another runtime, so a thread-local arm could never reach the
//! surface whose fail-closed behaviour is the point of the fix.
//!
//! Consequently — exactly like [`crate::storage::read_path_probe`]'s
//! process-global counters — **the caller must serialize**: one file = one test
//! binary = one process is the strongest form, a mutex held across the armed
//! window is the minimum. The arming functions deliberately return a plain
//! `Send` disarm-on-drop guard and take NO internal lock, so a guard may be held
//! across an `.await` without the `await_holding_lock` hazard; serialization is
//! the test's own explicit business.
//!
//! Two properties keep the failure modes loud rather than silent:
//!
//! * The OUTER arm is TAKEN (consumed) by the first [`ProducerFault::capture`],
//!   i.e. by exactly ONE stream, and is then owned by that stream's producer
//!   thread — never re-read from a shared cell mid-walk.
//! * A racing scan that stole an arm makes the arming test FAIL (it sees no
//!   error), never pass silently.

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
    /// Take whatever OUTER fault is armed for the next stream (nothing, in
    /// production).
    pub(crate) fn capture() -> Self {
        Self {
            #[cfg(any(test, feature = "producer-fault-injection"))]
            panic_after_batches: armed::take_outer(),
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

    /// Build an OUTER fault state directly, WITHOUT touching the process-global
    /// arm — so a unit test of this module's own logic can never race, or be
    /// raced by, a sibling test in the same binary.
    #[cfg(test)]
    fn for_test(panic_after_batches: u64) -> Self {
        Self {
            panic_after_batches: Some(panic_after_batches),
        }
    }
}

/// Consulted at the INNER batched-scan task's checkpoints — its cursor-open
/// prelude and every non-stitching block decode — so an armed fault unwinds THAT
/// task, the boundary whose disconnect the query-row thread used to read as "the
/// scan finished".
///
/// Panics ON PURPOSE when the armed budget is exhausted. Compiles to an empty
/// function in a production build.
#[inline]
pub(crate) fn inner_scan_task_checkpoint() {
    #[cfg(any(test, feature = "producer-fault-injection"))]
    armed::consume_inner_checkpoint();
}

/// The panic message both checkpoints raise. Exported so a test can (a) assert
/// the forwarded error carries it and (b) suppress exactly this panic in its
/// panic hook without silencing a real one.
#[cfg(any(test, feature = "producer-fault-injection"))]
pub const INJECTED_PANIC_MESSAGE: &str =
    "cqlite test fault injection (issue #3106): producer panic";

#[cfg(any(test, feature = "producer-fault-injection"))]
mod armed {
    use std::sync::atomic::{AtomicI64, Ordering};

    /// No fault armed.
    const DISARMED: i64 = -1;

    /// Batches the next opened query row stream may hand over before its producer
    /// thread panics, or [`DISARMED`]. TAKEN by `take_outer`, so exactly one
    /// stream can observe it.
    static OUTER_BATCHES: AtomicI64 = AtomicI64::new(DISARMED);

    /// Checkpoints the inner batched-scan task may pass before it panics, or
    /// [`DISARMED`]. Decremented in place (the checkpoint sites have no per-scan
    /// state to own it), which is why an arming caller must serialize — see the
    /// module doc.
    static INNER_CHECKPOINTS: AtomicI64 = AtomicI64::new(DISARMED);

    /// Saturate rather than wrap: an absurd budget simply never fires, and a
    /// negative value would read as `DISARMED`.
    fn as_budget(count: u64) -> i64 {
        i64::try_from(count).unwrap_or(i64::MAX)
    }

    pub(super) fn arm_outer(after_batches: u64) {
        OUTER_BATCHES.store(as_budget(after_batches), Ordering::SeqCst);
    }

    pub(super) fn disarm_outer() {
        OUTER_BATCHES.store(DISARMED, Ordering::SeqCst);
    }

    pub(super) fn take_outer() -> Option<u64> {
        u64::try_from(OUTER_BATCHES.swap(DISARMED, Ordering::SeqCst)).ok()
    }

    pub(super) fn arm_inner_checkpoints(after_checkpoints: u64) {
        INNER_CHECKPOINTS.store(as_budget(after_checkpoints), Ordering::SeqCst);
    }

    pub(super) fn disarm_inner_checkpoints() {
        INNER_CHECKPOINTS.store(DISARMED, Ordering::SeqCst);
    }

    /// Spend one checkpoint from the inner budget, panicking when it is
    /// exhausted. The budget is DISARMED before the panic so the unwind cannot
    /// re-enter and so a retry/sibling scan is never hit by the same arm.
    pub(super) fn consume_inner_checkpoint() {
        let remaining = INNER_CHECKPOINTS.load(Ordering::SeqCst);
        if remaining < 0 {
            return;
        }
        if remaining == 0 {
            disarm_inner_checkpoints();
            panic!("{}", super::INJECTED_PANIC_MESSAGE);
        }
        INNER_CHECKPOINTS.store(remaining - 1, Ordering::SeqCst);
    }
}

/// Arm the NEXT query row stream opened in this process to panic in its producer
/// THREAD just before it hands over batch number `after_batches` (0-based), so
/// `after_batches` batches reach the consumer and the walk then dies MID-STREAM.
///
/// `0` kills the producer before its first handoff. Disarmed when the returned
/// guard drops (and consumed by the stream that captures it). PROCESS-GLOBAL:
/// the caller serializes (see the module doc).
///
/// TEST-ONLY: this symbol does not exist unless `cfg(test)` or the
/// `producer-fault-injection` feature is on.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[must_use = "the fault stays armed only while the guard is alive"]
pub fn arm_query_row_producer_panic(after_batches: u64) -> ArmedProducerPanic {
    armed::arm_outer(after_batches);
    ArmedProducerPanic
}

/// Guard returned by [`arm_query_row_producer_panic`]: disarms on drop. Holds no
/// lock, so it is safe to hold across an `.await`.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[derive(Debug)]
pub struct ArmedProducerPanic;

#[cfg(any(test, feature = "producer-fault-injection"))]
impl Drop for ArmedProducerPanic {
    fn drop(&mut self) {
        armed::disarm_outer();
    }
}

/// Arm the INNER batched-scan task to panic at checkpoint number
/// `after_checkpoints` (0-based) inside itself, so the task unwinds and drops its
/// sender with no terminator.
///
/// `0` kills it in its cursor-open prelude — before any row, and independently of
/// which format branch the reader takes. Higher values are spent on the
/// non-stitching branch's per-block decodes (the `parse_block_entries_at_now`
/// call), so a reader whose format takes the stitching branch never reaches them.
///
/// Disarmed when the returned guard drops (and when the fault fires).
/// PROCESS-GLOBAL and decremented at the checkpoint sites, so the caller MUST
/// serialize against any sibling test that scans (see the module doc) — otherwise
/// the sibling's scan spends the budget.
///
/// TEST-ONLY: this symbol does not exist unless `cfg(test)` or the
/// `producer-fault-injection` feature is on.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[must_use = "the fault stays armed only while the guard is alive"]
pub fn arm_inner_scan_task_panic(after_checkpoints: u64) -> ArmedInnerScanPanic {
    armed::arm_inner_checkpoints(after_checkpoints);
    ArmedInnerScanPanic
}

/// Guard returned by [`arm_inner_scan_task_panic`]: disarms on drop. Holds no
/// lock, so it is safe to hold across an `.await`.
#[cfg(any(test, feature = "producer-fault-injection"))]
#[derive(Debug)]
pub struct ArmedInnerScanPanic;

#[cfg(any(test, feature = "producer-fault-injection"))]
impl Drop for ArmedInnerScanPanic {
    fn drop(&mut self) {
        armed::disarm_inner_checkpoints();
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
/// before (libtest's capture hook, normally), and that hook is reinstated on
/// drop.
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

/// Guard returned by [`silence_injected_panics`]; restores the previous hook.
#[cfg(any(test, feature = "producer-fault-injection"))]
pub struct SilencedInjectedPanics {
    previous: std::sync::Arc<PanicHook>,
}

#[cfg(any(test, feature = "producer-fault-injection"))]
impl Drop for SilencedInjectedPanics {
    fn drop(&mut self) {
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

    /// A default-constructed fault (the production shape) NEVER panics, however
    /// many batches are handed over.
    #[test]
    fn an_unarmed_producer_fault_is_inert() {
        let mut fault = ProducerFault::default();
        for _ in 0..1000 {
            fault.before_batch_handoff();
        }
    }

    /// The OUTER budget semantics, asserted WITHOUT touching the process-global
    /// arm (`for_test`), so this can never race a sibling test: the fault survives
    /// exactly its budget of handoffs, then panics.
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

    /// `capture` TAKES the process-global arm, so a fault can never leak into an
    /// unrelated later stream, and dropping the guard disarms. Single test = one
    /// critical section over the global (no sibling here arms it).
    #[test]
    fn the_global_arm_is_taken_by_one_capture_and_dropped_with_the_guard() {
        let guard = arm_query_row_producer_panic(7);
        let first = ProducerFault::capture();
        let mut second = ProducerFault::capture();
        assert_eq!(
            first.panic_after_batches,
            Some(7),
            "the first capture takes the armed budget"
        );
        assert_eq!(
            second.panic_after_batches, None,
            "a second stream under the same arm is clean"
        );
        for _ in 0..10 {
            second.before_batch_handoff();
        }

        drop(guard);
        assert_eq!(
            ProducerFault::capture().panic_after_batches,
            None,
            "dropping the guard disarms, so a later stream is never poisoned"
        );
    }
}
