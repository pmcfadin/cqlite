//! Panic-hook silencing for the injected faults (issues #3106/#3124), split out of
//! [`super`] so that file stays under the ~800-line campsite target (epic #1116).
//!
//! The whole module is gated by its `mod` declaration in [`super`] — it exists only
//! where a fault can be armed at all — so no item below repeats that `cfg`.

use super::INJECTED_PANIC_MESSAGE;

/// A boxed panic hook, as [`std::panic::set_hook`] takes it.
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
pub struct SilencedInjectedPanics {
    previous: std::sync::Arc<PanicHook>,
}

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
