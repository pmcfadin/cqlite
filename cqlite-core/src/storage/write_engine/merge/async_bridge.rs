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
//! A single long-lived bridge runtime is built lazily on first *successful* use
//! and reused for every subsequent call (Issue #1670) — previously a fresh
//! `Runtime::new()` was constructed and torn down on *every* flush/maintenance
//! step. A transient build failure is NOT cached: the error is returned to the
//! caller and the next call retries, so one bad build never disables the bridge
//! for the process lifetime. The cached runtime is a **multi-thread** runtime (worker threads =
//! CPU count) with all drivers (I/O, time, blocking pool) enabled — the same
//! flavor `Runtime::new()` built before #1670. This is deliberate: concurrent
//! flush/compaction each bridge through `block_on_async`, and a single shared
//! `current_thread` runtime would serialize those concurrent `block_on` calls
//! (and any internal task fan-out within one future), regressing
//! concurrent-flush/compaction throughput vs main. A multi-thread runtime
//! preserves main's parallelism semantics: N threads can each drive a
//! `block_on` on the shared runtime and its worker pool services them in
//! parallel.
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
use std::sync::{Mutex, OnceLock};
#[cfg(feature = "write-support")]
use tokio::runtime::Runtime;

/// Process-wide cached bridge runtime. Built once on first *successful* use and
/// reused for the life of the process (Issue #1670). Tests that create and drop
/// many `WriteEngine`s reuse this single runtime — a process-global runtime that
/// outlives every engine is intentional and harmless.
///
/// The cell holds ONLY a successfully-built [`Runtime`] (not a
/// `Result<Runtime, _>`). A transient build failure (e.g. thread-spawn/resource
/// exhaustion in [`tokio::runtime::Builder::build`]) returns an error to the
/// caller WITHOUT populating this cell, so the next call retries — a failed
/// build never poisons the cache for the process lifetime. Builds are serialized
/// by [`BRIDGE_BUILD_LOCK`] with a double-check so a successful build still
/// happens at most once even under a concurrent first-access race.
#[cfg(feature = "write-support")]
static BRIDGE_RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Serializes bridge-runtime construction so that (a) at most one build is
/// in-flight at a time and (b) a successful build populates [`BRIDGE_RUNTIME`]
/// exactly once even when multiple threads miss the fast path simultaneously.
/// A single lock acquisition per bridge call is negligible next to the ~290µs a
/// per-call `Runtime::new()` cost before #1670.
#[cfg(feature = "write-support")]
static BRIDGE_BUILD_LOCK: Mutex<()> = Mutex::new(());

/// Number of times the cached bridge runtime has actually been constructed.
///
/// Incremented immediately before each real `Runtime` build (under the build
/// lock), so it counts actual build *attempts* — not `block_on_async` calls.
/// With the cache it is `<= 1` for the life of the process once a build
/// succeeds; before Issue #1670 (a `Runtime::new()` per call) it grew by one on
/// every `block_on_async` invocation. A retried build after a transient failure
/// increments it again — the intended, observable retry behavior.
#[cfg(feature = "write-support")]
static RUNTIME_BUILD_COUNT: AtomicU64 = AtomicU64::new(0);

/// Return the cached bridge runtime, building it on first use.
///
/// A failed build returns `Err` WITHOUT caching anything, so the next call
/// retries (no permanent poison — the regression fixed for Issue #1670). Builds
/// are serialized by [`BRIDGE_BUILD_LOCK`] with a re-check under the lock, so a
/// successful build happens at most once even if many threads miss the fast
/// path at the same instant.
#[cfg(feature = "write-support")]
fn bridge_runtime() -> Result<&'static Runtime> {
    get_or_build_once(&BRIDGE_RUNTIME, &BRIDGE_BUILD_LOCK, || {
        RUNTIME_BUILD_COUNT.fetch_add(1, Ordering::Relaxed);
        // Multi-thread flavor (worker threads = CPU count), matching main's
        // `Runtime::new()` semantics so concurrent `block_on_async` calls and
        // in-future task fan-out run in parallel rather than being serialized by
        // a shared single-threaded scheduler (Issue #1670).
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Storage(format!("Failed to create tokio runtime: {}", e)))
    })
}

/// Return the value cached in `cell`, building it via `build` on the first miss.
///
/// The store-only-on-success cache contract (Issue #1670 robustness fix):
/// - Fast path returns the cached value with no locking once populated.
/// - On a miss, `lock` serializes construction and a re-check avoids a double
///   build under a concurrent first-access race, so a successful build happens
///   **at most once**.
/// - If `build` returns `Err`, the error is propagated and `cell` stays EMPTY,
///   so a later call retries — a transient failure never permanently poisons
///   the cache.
#[cfg(feature = "write-support")]
fn get_or_build_once<T, F>(
    cell: &'static OnceLock<T>,
    lock: &Mutex<()>,
    build: F,
) -> Result<&'static T>
where
    F: FnOnce() -> Result<T>,
{
    // Fast path: already built and cached.
    if let Some(v) = cell.get() {
        return Ok(v);
    }
    // Slow path: serialize construction so at most one build runs at a time.
    let _guard = lock
        .lock()
        .map_err(|_| Error::Storage("bridge runtime build lock poisoned".to_string()))?;
    // Re-check under the lock: another thread may have built while we waited.
    if let Some(v) = cell.get() {
        return Ok(v);
    }
    // On `Err` we return here WITHOUT touching `cell`, so it stays empty and the
    // next call retries the build (no permanent poison).
    let v = build()?;
    // Only reached on success. We hold the lock and re-checked emptiness above,
    // so `set` succeeds; store then return the stored ref.
    if cell.set(v).is_err() {
        return Err(Error::Storage(
            "bridge runtime unexpectedly already set".to_string(),
        ));
    }
    cell.get()
        .ok_or_else(|| Error::Storage("bridge runtime missing after successful set".to_string()))
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

    /// Issue #1670: concurrent `block_on_async` calls from many OS threads on
    /// the single shared runtime must all complete correctly and must NOT
    /// serialize. Each future spawns inner tasks (task fan-out) and awaits a
    /// short timer; a multi-thread runtime services them on its worker pool in
    /// parallel. A shared `current_thread` runtime would serialize both the
    /// top-level `block_on` calls and the inner fan-out — so this both proves
    /// thread-safety of the shared handle and guards the multi-thread flavor.
    #[test]
    fn concurrent_block_on_from_many_threads_is_safe_and_parallel() {
        use std::sync::Arc;

        const THREADS: usize = 8;
        // Prime the cache from the no-runtime path so all worker threads reuse
        // one runtime rather than racing to build it.
        let _: Result<u64> = block_on_async(async { Ok(0u64) });
        let build_before = runtime_build_count();

        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        let start = std::time::Instant::now();
        let handles: Vec<_> = (0..THREADS)
            .map(|i| {
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    block_on_async(async move {
                        // Inner task fan-out: joined concurrently on the shared
                        // worker pool, then a short timer to keep each future
                        // in-flight while its peers run.
                        let a = tokio::spawn(async move { i });
                        let b = tokio::spawn(async move { i * 2 });
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        let (a, b) = (
                            a.await.map_err(|e| Error::Storage(e.to_string()))?,
                            b.await.map_err(|e| Error::Storage(e.to_string()))?,
                        );
                        Ok::<u64, Error>((a + b) as u64)
                    })
                })
            })
            .collect();

        for (i, h) in handles.into_iter().enumerate() {
            let got = h
                .join()
                .expect("worker thread did not panic")
                .expect("concurrent block_on_async succeeds");
            assert_eq!(got, (i * 3) as u64, "wrong result on thread {i}");
        }

        // All THREADS futures sleep 100ms and ran concurrently on the shared
        // runtime; if they were serialized (a single-threaded scheduler) the
        // wall time would approach THREADS * 100ms. A generous ceiling well
        // below that proves non-serialization without being flake-prone.
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_millis(100 * THREADS as u64 / 2),
            "concurrent calls appear serialized: {elapsed:?} for {THREADS} x 100ms sleeps"
        );
        // Still the single cached runtime — no per-thread rebuild.
        assert_eq!(
            runtime_build_count(),
            build_before,
            "shared runtime was rebuilt under concurrent load"
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

    /// Issue #1670 robustness: a FAILED build must NOT poison the cache — the
    /// next call retries and can succeed. Exercised on the isolated
    /// `get_or_build_once` helper (its own `OnceLock`/`Mutex`) so the property is
    /// tested without brittle global state or a way to force a real tokio build
    /// to fail. This is the regression roborev flagged: a permanently-cached
    /// `Err` would disable the bridge for the whole process.
    #[test]
    fn failed_build_does_not_poison_cache_and_retries() {
        use std::sync::atomic::AtomicU32;

        static CELL: OnceLock<u32> = OnceLock::new();
        static LOCK: Mutex<()> = Mutex::new(());
        static ATTEMPTS: AtomicU32 = AtomicU32::new(0);

        // First call: the builder fails. The cell must stay empty (not poisoned).
        let first: Result<&'static u32> = get_or_build_once(&CELL, &LOCK, || {
            ATTEMPTS.fetch_add(1, Ordering::Relaxed);
            Err(Error::Storage("transient build failure".to_string()))
        });
        assert!(first.is_err(), "first build should surface the error");
        assert!(
            CELL.get().is_none(),
            "failed build must leave the cache empty (no poison)"
        );

        // Second call: the builder succeeds and the value is cached.
        let second: Result<&'static u32> = get_or_build_once(&CELL, &LOCK, || {
            ATTEMPTS.fetch_add(1, Ordering::Relaxed);
            Ok(42u32)
        });
        assert_eq!(*second.expect("retry after failure succeeds"), 42);
        assert_eq!(CELL.get(), Some(&42), "successful build is cached");

        // Third call: fast path, no further build attempts.
        let third: Result<&'static u32> =
            get_or_build_once(&CELL, &LOCK, || panic!("must not rebuild after success"));
        assert_eq!(*third.expect("cached value returned"), 42);

        // Exactly two build attempts: one failed, one succeeded — proving the
        // retry happened and the success was not rebuilt.
        assert_eq!(ATTEMPTS.load(Ordering::Relaxed), 2);
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
