//! Async-to-sync bridge for the write engine's blocking helpers (Issue #587,
//! #1670).
//!
//! `block_on_async` runs an async future to completion from a synchronous
//! context, safely whether or not a Tokio runtime is already running on the
//! current thread. It is the shared bridge for `SSTableRowIteratorAdapter` (the
//! k-way merge readers), `WriteEngine::flush_internal`, and
//! `WriteEngine::finalize_merge_blocking`.
//!
//! ## Why not `Handle::block_on`?
//!
//! When this bridge is reached from a thread that is already driving a Tokio
//! runtime — anything under `#[tokio::main]` or `#[tokio::test]`, which is how
//! the CLI (`maintenance`, `export-sstable --compact`) and any async caller
//! reach compaction — `Handle::current().block_on()` panics with *"Cannot start
//! a runtime from within a runtime"* (Issue #587). Compaction only reaches the
//! bridge once a merge has input SSTables to read, which is why STCS worked in
//! isolation but blew up from async callers.
//!
//! `tokio::task::block_in_place` is not a general fix either: it panics on a
//! current-thread runtime (e.g. the default `#[tokio::test]` flavor).
//!
//! ## Strategy
//!
//! A single long-lived bridge runtime is built lazily on first use and reused
//! for every subsequent call (Issue #1670) — previously a fresh
//! `Runtime::new()` was constructed and torn down on *every* flush/maintenance
//! step. The cached runtime is a cheap `current_thread` flavor (it only ever
//! drives one `block_on` at a time per calling thread) with all drivers
//! (I/O, time, blocking pool) enabled.
//!
//! - **No runtime on the current thread** (`Handle::try_current()` is `Err`):
//!   block on the cached runtime directly. A panic in the future unwinds to the
//!   caller (catchable via `catch_unwind`; the host does not abort) exactly as
//!   before, and the cached runtime remains usable afterward (Tokio restores
//!   its scheduler core on unwind).
//! - **Already inside a runtime** (`Ok`): a nested `block_on` on this thread
//!   would panic, so the future is handed to a dedicated scoped thread that
//!   drives the *cached* runtime (a `&'static Runtime` is `Send`, so the scoped
//!   thread can call `block_on` on it while the caller's runtime keeps its own
//!   thread). The thread is free to block because it is not driving the
//!   caller's runtime, so this works for both the multi-thread and
//!   current-thread runtime flavors. `std::thread::scope` (rather than
//!   `std::thread::spawn`) lets the future borrow from the caller's stack —
//!   `flush_internal`/`finalize_merge_blocking` pass futures that borrow
//!   `&mut self` — so it need not be `'static`. The scoped-thread `join()`
//!   catches a panic in the future and surfaces it as `Error::Storage`
//!   (Issue #587 panic-safety), instead of unwinding across the FFI/host.
//!
//! The future and its output must be `Send` because they cross a thread
//! boundary in the in-runtime case.

#[cfg(feature = "write-support")]
use crate::error::{Error, Result};
#[cfg(feature = "write-support")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "write-support")]
use std::sync::OnceLock;
#[cfg(feature = "write-support")]
use tokio::runtime::Runtime;

/// Process-wide cached bridge runtime. Built once on first use and reused for
/// the life of the process (Issue #1670). Tests that create and drop many
/// `WriteEngine`s reuse this single runtime — a process-global runtime that
/// outlives every engine is intentional and harmless.
///
/// Stored as `Result<Runtime, String>` because `OnceLock::get_or_init` cannot
/// return a fallible value on stable Rust and `get_or_try_init` is unstable; on
/// a build failure the (rare) error message is cloned into an [`Error::Storage`]
/// on every call, preserving the original error text.
#[cfg(feature = "write-support")]
static BRIDGE_RUNTIME: OnceLock<std::result::Result<Runtime, String>> = OnceLock::new();

/// Number of times the cached bridge runtime has actually been constructed.
///
/// Incremented exactly inside the [`BRIDGE_RUNTIME`] initializer, so it counts
/// real `Runtime` builds — not `block_on_async` calls. With the cache it is
/// `<= 1` for the life of the process; before Issue #1670 (a `Runtime::new()`
/// per call) it grew by one on every `block_on_async` invocation.
#[cfg(feature = "write-support")]
static RUNTIME_BUILD_COUNT: AtomicU64 = AtomicU64::new(0);

/// Return the cached bridge runtime, building it on first use.
#[cfg(feature = "write-support")]
fn bridge_runtime() -> Result<&'static Runtime> {
    let built = BRIDGE_RUNTIME.get_or_init(|| {
        RUNTIME_BUILD_COUNT.fetch_add(1, Ordering::Relaxed);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create tokio runtime: {}", e))
    });
    match built {
        Ok(rt) => Ok(rt),
        Err(msg) => Err(Error::Storage(msg.clone())),
    }
}

/// Run an async future to completion from a synchronous context, safely whether
/// or not a Tokio runtime is already running on the current thread. See the
/// module docs for the full strategy and Issue #587 / #1670 rationale.
#[cfg(feature = "write-support")]
pub(crate) fn block_on_async<F, T>(future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send,
    T: Send,
{
    let rt = bridge_runtime()?;
    match tokio::runtime::Handle::try_current() {
        // Already inside a runtime: a nested `block_on` on this thread would
        // panic. Drive the cached runtime from a scoped thread instead; its
        // `join()` also catches a panic in the future (Issue #587).
        Ok(_) => std::thread::scope(|scope| {
            scope
                .spawn(|| rt.block_on(future))
                .join()
                .map_err(|_| Error::Storage("async-to-sync bridge thread panicked".to_string()))?
        }),
        // No runtime on this thread: block on the cached runtime directly.
        Err(_) => rt.block_on(future),
    }
}

#[cfg(all(test, feature = "write-support"))]
mod tests {
    use super::*;

    /// Read the current construction count (test-only accessor).
    fn runtime_build_count() -> u64 {
        RUNTIME_BUILD_COUNT.load(Ordering::Relaxed)
    }

    /// Issue #1670 TDD: N `block_on_async` calls must construct the runtime at
    /// most ONCE (it is cached), exercising BOTH the no-runtime path and the
    /// in-runtime (scoped-thread) path. On main (a `Runtime::new()` per call at
    /// the old lines 367/377) this count would equal the number of calls
    /// (`>= N`, here 16), so the `<= 1` assertion is red-on-main.
    #[test]
    fn runtime_is_constructed_at_most_once_across_many_calls() {
        // No-runtime path: N direct calls.
        for i in 0..8u64 {
            let out: Result<u64> = block_on_async(async move { Ok(i) });
            assert_eq!(out.expect("no-runtime call succeeds"), i);
        }

        // In-runtime path: N calls from inside an ambient runtime (each hits the
        // scoped-thread branch).
        let ambient = tokio::runtime::Runtime::new().expect("ambient runtime");
        ambient.block_on(async {
            assert!(tokio::runtime::Handle::try_current().is_ok());
            for i in 100..108u64 {
                let out: Result<u64> = block_on_async(async move { Ok(i) });
                assert_eq!(out.expect("in-runtime call succeeds"), i);
            }
        });

        assert!(
            runtime_build_count() <= 1,
            "bridge runtime constructed {} times across 16 calls; expected <= 1 (cached)",
            runtime_build_count()
        );
    }

    /// Issue #587 panic-safety, in-runtime path: a panic in the future must be
    /// caught by the scoped-thread `join()` and surface as `Err(Error::Storage)`
    /// — never unwind across the caller's runtime / the host.
    #[test]
    fn in_runtime_future_panic_returns_err_not_abort() {
        let ambient = tokio::runtime::Runtime::new().expect("ambient runtime");
        ambient.block_on(async {
            let result: Result<u64> = block_on_async(async {
                panic!("boom in future (in-runtime path)");
                #[allow(unreachable_code)]
                Ok(0u64)
            });
            match result {
                Err(Error::Storage(msg)) => {
                    assert!(msg.contains("panicked"), "unexpected message: {msg}")
                }
                other => panic!("expected Err(Storage(..)), got {other:?}"),
            }
        });
    }

    /// Issue #587 panic-safety, no-runtime path: a panic in the future unwinds
    /// to the caller (the direct-block-on branch does NOT convert it to `Err`),
    /// but it is catchable via `catch_unwind` — the host does not abort — and
    /// the cached runtime stays usable afterward. This preserves the exact
    /// pre-#1670 semantics of the direct branch.
    #[test]
    fn no_runtime_future_panic_is_catchable_and_runtime_survives() {
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "test must run without an ambient runtime"
        );
        let caught = std::panic::catch_unwind(|| {
            let _: Result<u64> = block_on_async(async {
                panic!("boom in future (no-runtime path)");
                #[allow(unreachable_code)]
                Ok(0u64)
            });
        });
        assert!(
            caught.is_err(),
            "panic should be catchable, host must survive"
        );

        // Cached runtime is still usable after the unwind.
        let after: Result<u64> = block_on_async(async { Ok(42u64) });
        assert_eq!(after.expect("runtime usable after panic"), 42);
    }
}
