//! Tokio runtime management for Node.js bindings.
//!
//! Provides a global tokio runtime singleton for bridging async Rust
//! operations to synchronous napi-rs task execution.
//!
//! This implementation mirrors `bindings/python/src/runtime.rs` for
//! consistency across language bindings per M4 spec Section 2.1.

use std::future::Future;
use std::sync::{Mutex, OnceLock};
use tokio::runtime::Runtime;

/// Global tokio runtime instance.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Serializes the fallible slow-path build so at most one runtime is ever
/// constructed, even under concurrent first use.
static INIT_LOCK: Mutex<()> = Mutex::new(());

/// Returns a reference to the global tokio runtime, building it on first use.
///
/// The runtime is lazily initialized on first access using a multi-threaded
/// executor with all features enabled. On success the runtime is memoized in a
/// process-global `OnceLock` and reused by every subsequent call.
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] if the runtime cannot be created
/// (e.g., the host is out of threads, file descriptors, or memory). Unlike a
/// panicking initializer, a failed build does **not** poison the cell: a later
/// call can retry once resources are available, and the error is surfaced to the
/// caller (mapped to a `napi::Error` at the binding boundary so it rejects/throws)
/// rather than aborting the host process under `panic = "abort"`.
pub fn try_get_runtime() -> Result<&'static Runtime, std::io::Error> {
    get_or_try_init(&RUNTIME, &INIT_LOCK, || {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("cqlite-node-worker")
            .enable_all()
            .build()
    })
}

/// Serialized get-or-build for a process-global [`OnceLock`].
///
/// Preserves `OnceLock::get_or_init`'s single-initializer guarantee while
/// allowing the initializer to be fallible: at most one `build` ever runs, even
/// under concurrent first callers, and a build failure leaves the cell empty
/// (no poisoning) so a later call can retry.
///
/// Shape: fast-path `get()`, then take `lock` (serializing the slow path),
/// re-check `get()` under the lock (a thread that lost the race returns the
/// already-built value instead of building a second one), then build + `set`.
fn get_or_try_init<T, F>(
    cell: &'static OnceLock<T>,
    lock: &'static Mutex<()>,
    build: F,
) -> Result<&'static T, std::io::Error>
where
    F: FnOnce() -> Result<T, std::io::Error>,
{
    // Fast path: already built and memoized.
    if let Some(v) = cell.get() {
        return Ok(v);
    }

    // Slow path: serialize construction so concurrent first callers do not each
    // run a competing `build` (which, under the very resource pressure this
    // change handles, could make one caller spuriously fail while another
    // succeeds). Recover from a poisoned lock instead of panicking — the guard
    // protects no data, so the lock is still usable.
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    // Re-check under the lock: another thread may have installed the value while
    // we waited on the lock.
    if let Some(v) = cell.get() {
        return Ok(v);
    }

    // Build fallibly. A failed build leaves the cell empty (no poisoning), so a
    // later call can retry once resources are available.
    let v = build()?;
    // We hold the lock and confirmed the cell was empty, so this always installs.
    let _ = cell.set(v);
    match cell.get() {
        Some(v) => Ok(v),
        // Unreachable: we just set the cell while holding the init lock. Surface
        // a plain error rather than panic to keep this function total (no
        // unwrap/expect in library code).
        None => Err(std::io::Error::other(
            "OnceLock unexpectedly empty after serialized initialization",
        )),
    }
}

/// Executes an async future on the global runtime, blocking until completion.
///
/// This is the primary bridge between napi-rs sync tasks and async Rust operations.
///
/// # Errors
///
/// Returns the [`std::io::Error`] from [`try_get_runtime`] if the shared runtime
/// could not be created. On the success path this is identical to the previous
/// behavior: the runtime is built once and reused.
pub fn block_on<F: Future>(future: F) -> Result<F::Output, std::io::Error> {
    Ok(try_get_runtime()?.block_on(future))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_get_runtime_ok_and_memoized() {
        // Success path returns a usable runtime and memoizes it: the second call
        // yields the very same instance (issue #1438).
        let rt1 = try_get_runtime().expect("runtime should build in tests");
        let rt2 = try_get_runtime().expect("runtime should build in tests");
        assert!(std::ptr::eq(rt1, rt2));
    }

    #[test]
    fn test_try_get_runtime_returns_result() {
        // The contract that replaces the old panicking `.expect()`: init is
        // fallible and returns a `Result`, so a resource-starved host surfaces a
        // catchable error at the binding boundary instead of aborting the process.
        let rt: Result<&'static Runtime, std::io::Error> = try_get_runtime();
        assert!(rt.is_ok());
    }

    #[test]
    fn test_block_on_executes_async() {
        let result = block_on(async { 42 }).expect("runtime available");
        assert_eq!(result, 42);
    }

    #[test]
    fn test_block_on_with_async_block() {
        let result = block_on(async {
            let a = 10;
            let b = 20;
            a + b
        })
        .expect("runtime available");
        assert_eq!(result, 30);
    }

    #[test]
    fn test_runtime_from_multiple_threads() {
        use std::thread;

        let handles: Vec<_> = (0..4)
            .map(|_| {
                thread::spawn(|| {
                    let rt = try_get_runtime().expect("runtime should build in tests");
                    std::ptr::addr_of!(*rt) as usize
                })
            })
            .collect();

        let addresses: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All threads should get the same runtime instance
        assert!(addresses.windows(2).all(|w| w[0] == w[1]));
    }

    #[test]
    fn test_serialized_slow_path_builds_at_most_once() {
        // Prove the fallible slow path retains `OnceLock::get_or_init`'s
        // single-initializer guarantee: N threads all hit the cold cell together
        // yet exactly ONE `build` runs, and every caller observes the SAME
        // `&'static T` (pointer identity). This is the fix for issue #1438's
        // Finding 1 — the earlier get()-then-build()?-then-set() shape could
        // build a separate runtime per racing first caller.
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};
        use std::thread;

        static CELL: OnceLock<usize> = OnceLock::new();
        static LOCK: Mutex<()> = Mutex::new(());
        static BUILDS: AtomicUsize = AtomicUsize::new(0);

        const N: usize = 8;
        let barrier = Arc::new(Barrier::new(N));
        let handles: Vec<_> = (0..N)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    // Release all threads simultaneously so they contend on the
                    // cold cell / init lock at once.
                    barrier.wait();
                    let v = get_or_try_init(&CELL, &LOCK, || {
                        BUILDS.fetch_add(1, Ordering::SeqCst);
                        Ok(0xC0FFEE_usize)
                    })
                    .expect("serialized init should succeed");
                    std::ptr::addr_of!(*v) as usize
                })
            })
            .collect();

        let addresses: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Exactly one build ran despite N concurrent first callers.
        assert_eq!(BUILDS.load(Ordering::SeqCst), 1, "build must run exactly once");
        // Every caller observed the same &'static value.
        assert!(
            addresses.windows(2).all(|w| w[0] == w[1]),
            "all callers must observe the same instance"
        );
    }
}
