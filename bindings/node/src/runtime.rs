//! Tokio runtime management for Node.js bindings.
//!
//! Provides a global tokio runtime singleton for bridging async Rust
//! operations to synchronous napi-rs task execution.
//!
//! This implementation mirrors `bindings/python/src/runtime.rs` for
//! consistency across language bindings per M4 spec Section 2.1.

use std::future::Future;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// Global tokio runtime instance.
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// Returns a reference to the global tokio runtime.
///
/// The runtime is lazily initialized on first access using a multi-threaded
/// executor with all features enabled.
///
/// # Panics
///
/// Panics if the runtime cannot be created (e.g., system resource exhaustion).
/// This use of `expect()` is acceptable because:
///
/// 1. **Module initialization context**: Failure occurs once at module load time,
///    not during normal operations
/// 2. **Fatal condition**: Runtime creation only fails under extreme resource
///    exhaustion (no memory, file descriptors, or thread capacity)
/// 3. **No recovery path**: All CQLite Node.js operations require an async runtime;
///    there is no meaningful fallback
/// 4. **Clear error message**: Users see the panic message as a module load failure
///
/// In Node.js, this manifests as a thrown error during `require('@cqlite/node')`,
/// which is the appropriate failure mode for an unrecoverable initialization error.
pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name("cqlite-node-worker")
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime - system may be out of resources")
    })
}

/// Executes an async future on the global runtime, blocking until completion.
///
/// This is the primary bridge between napi-rs sync tasks and async Rust operations.
///
/// # Arguments
///
/// * `future` - The async operation to execute
///
/// # Returns
///
/// The result of the future
pub fn block_on<F: Future>(future: F) -> F::Output {
    get_runtime().block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_initializes_once() {
        let rt1 = get_runtime();
        let rt2 = get_runtime();
        assert!(std::ptr::eq(rt1, rt2));
    }

    #[test]
    fn test_block_on_executes_async() {
        let result = block_on(async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_block_on_with_async_block() {
        let result = block_on(async {
            let a = 10;
            let b = 20;
            a + b
        });
        assert_eq!(result, 30);
    }

    #[test]
    fn test_runtime_from_multiple_threads() {
        use std::thread;

        let handles: Vec<_> = (0..4)
            .map(|_| {
                thread::spawn(|| {
                    let rt = get_runtime();
                    std::ptr::addr_of!(*rt) as usize
                })
            })
            .collect();

        let addresses: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All threads should get the same runtime instance
        assert!(addresses.windows(2).all(|w| w[0] == w[1]));
    }
}
