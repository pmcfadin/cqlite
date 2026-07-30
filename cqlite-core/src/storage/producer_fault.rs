//! TEST-ONLY producer-fault injection for the query row stream (issue #3106).
//!
//! # Why this exists
//!
//! The query row stream ([`crate::storage::sstable::reader::QueryRowStream`]) is
//! fed by a detached producer thread over a bounded channel. Before issue #3106
//! the consumer collapsed a channel DISCONNECT into a clean end of stream, so a
//! producer thread that UNWOUND (a panic anywhere in the walk/decode, rather than
//! an `Err` return) dropped its `SyncSender` without a terminal message and the
//! request completed SUCCESSFULLY with a silently truncated result set. The fix
//! makes completion explicit (a `Done` sentinel) and forwards a caught panic as a
//! real error; this module is how a test PROVES that, deterministically, without
//! waiting for a real decode bug to unwind.
//!
//! # Why this is not a production knob (no-heuristics safe)
//!
//! * Everything that can arm a fault is `#[cfg(any(test, feature =
//!   "producer-fault-injection"))]`. In a default build the arming API does not
//!   exist, the armed-state statics do not exist, [`ProducerFault`] is a
//!   zero-sized struct with no fields, and [`ProducerFault::before_batch_handoff`]
//!   compiles to an empty function — the production build is byte-identical to
//!   one without this module's body.
//! * No environment variable, config field or on-disk byte pattern can arm it:
//!   the only way in is a Rust call to [`arm_query_row_producer_panic`], which is
//!   compiled out of production builds. So it cannot influence a decoding
//!   decision (issue #28) even accidentally.
//! * `cqlite-flight` enables the feature from its `[dev-dependencies]` (the same
//!   convention `arrow-shape-corpus` / `test-util` already use), so the shipped
//!   Flight binary never links it.
//!
//! # Arming semantics
//!
//! [`arm_query_row_producer_panic`] arms the NEXT query row stream opened
//! ANYWHERE in the process — process-global rather than thread-local on purpose:
//! the Flight `do_get` row route opens its stream on a `spawn_blocking` thread,
//! not on the calling thread, so a thread-local arm could never reach the surface
//! whose fail-closed behaviour is the point of the fix. To keep that
//! deterministic:
//!
//! * The arm is TAKEN (consumed) by the first [`ProducerFault::capture`], i.e. by
//!   exactly ONE stream, and is owned by that stream's producer thread from then
//!   on — never re-read from a shared cell mid-walk.
//! * [`arm_query_row_producer_panic`] holds a process-global lock for the
//!   returned guard's lifetime, so two arming tests in one binary serialize.
//! * A racing NON-arming stream that stole the arm makes the arming test FAIL
//!   (it sees no error), never pass silently — the failure mode is loud.

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
    /// Take whatever fault is armed for the next stream (nothing, in production).
    pub(crate) fn capture() -> Self {
        Self {
            #[cfg(any(test, feature = "producer-fault-injection"))]
            panic_after_batches: armed::take(),
        }
    }

    /// Consulted by the producer immediately BEFORE it hands a batch to the
    /// consumer — the single batch-handoff funnel both walk arms go through, so
    /// an injected fault is observed identically on either.
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
}

/// The panic message [`ProducerFault::before_batch_handoff`] raises. Exported so
/// a test can (a) assert the forwarded error carries it and (b) suppress exactly
/// this panic in its panic hook without silencing a real one.
#[cfg(any(test, feature = "producer-fault-injection"))]
pub const INJECTED_PANIC_MESSAGE: &str =
    "cqlite test fault injection (issue #3106): query row stream producer thread panic";

#[cfg(any(test, feature = "producer-fault-injection"))]
mod armed {
    use std::sync::atomic::{AtomicI64, Ordering};

    /// No fault armed.
    const DISARMED: i64 = -1;

    /// Batches the next opened stream may hand over before panicking, or
    /// [`DISARMED`]. Written only by the arming API / its guard, and TAKEN by
    /// `take` so exactly one stream can observe it.
    static ARMED_BATCHES: AtomicI64 = AtomicI64::new(DISARMED);

    /// Serializes arming tests in one binary (see the module doc).
    static ARM_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    pub(super) fn arm(after_batches: u64) -> parking_lot::MutexGuard<'static, ()> {
        let lock = ARM_LOCK.lock();
        // Saturate rather than wrap: an absurd budget simply never fires, and a
        // negative value would read as DISARMED.
        ARMED_BATCHES.store(
            i64::try_from(after_batches).unwrap_or(i64::MAX),
            Ordering::SeqCst,
        );
        lock
    }

    pub(super) fn disarm() {
        ARMED_BATCHES.store(DISARMED, Ordering::SeqCst);
    }

    pub(super) fn take() -> Option<u64> {
        let armed = ARMED_BATCHES.swap(DISARMED, Ordering::SeqCst);
        u64::try_from(armed).ok()
    }
}

/// Arm the NEXT query row stream opened in this process to panic in its producer
/// thread just before it hands over batch number `after_batches` (0-based), so
/// `after_batches` batches reach the consumer and the walk then dies MID-STREAM.
///
/// `0` kills the producer before its first handoff. The arm is disarmed when the
/// returned guard is dropped (and consumed by the stream that captures it), and
/// the guard serializes against any other arming caller in the same test binary.
///
/// TEST-ONLY: this symbol does not exist unless `cfg(test)` or the
/// `producer-fault-injection` feature is on (see the module doc).
#[cfg(any(test, feature = "producer-fault-injection"))]
#[must_use = "the fault stays armed only while the guard is alive"]
pub fn arm_query_row_producer_panic(after_batches: u64) -> ArmedProducerPanic {
    ArmedProducerPanic {
        _lock: armed::arm(after_batches),
    }
}

/// Guard returned by [`arm_query_row_producer_panic`]: disarms the fault and
/// releases the arming lock on drop.
#[cfg(any(test, feature = "producer-fault-injection"))]
pub struct ArmedProducerPanic {
    _lock: parking_lot::MutexGuard<'static, ()>,
}

#[cfg(any(test, feature = "producer-fault-injection"))]
impl Drop for ArmedProducerPanic {
    fn drop(&mut self) {
        armed::disarm();
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

/// Whether `info` describes the panic [`ProducerFault::before_batch_handoff`]
/// raises. Matched on the payload STRING, so no real panic is ever swallowed.
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

    /// The arming contract, asserted in ONE critical section (a second arming
    /// test would race this one for the process-global arm between its own
    /// `arm`/`capture` calls):
    ///
    /// * the armed stream survives exactly its budget of handoffs, then panics;
    /// * `capture` TAKES the arm, so a second stream opened under the same arm is
    ///   clean — a fault can never leak into an unrelated later stream;
    /// * dropping the guard disarms, so a finished test cannot poison a later one.
    #[test]
    fn arming_applies_to_exactly_one_captured_stream_and_is_dropped_with_the_guard() {
        let guard = arm_query_row_producer_panic(2);
        let mut first = ProducerFault::capture();
        let mut second = ProducerFault::capture();

        // The armed stream survives exactly two handoffs, then dies. The panic
        // is EXPECTED here, so the hook is silenced for the window and restored
        // before any assertion runs (a silenced hook would hide a real failure
        // message).
        first.before_batch_handoff();
        first.before_batch_handoff();
        let previous_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let died = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            first.before_batch_handoff();
        }));
        std::panic::set_hook(previous_hook);
        assert!(
            died.is_err(),
            "the armed stream must panic on the handoff after its budget"
        );

        // The second capture got nothing: the arm was already taken.
        for _ in 0..10 {
            second.before_batch_handoff();
        }

        // And once the guard is gone, nothing is armed any more.
        drop(guard);
        let mut later = ProducerFault::capture();
        for _ in 0..10 {
            later.before_batch_handoff();
        }
    }
}
