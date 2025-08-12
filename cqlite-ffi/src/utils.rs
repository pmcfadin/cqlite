//! FFI utilities
//!
//! This module contains utility functions for the FFI interface.

use crate::error::{CQLITE_ERROR_INIT, set_last_error};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::runtime::Runtime;

/// Global runtime storage
static RUNTIME: OnceLock<Arc<Mutex<Option<Arc<Runtime>>>>> = OnceLock::new();

/// Get or create the global async runtime
pub fn get_or_create_runtime() -> Result<Arc<Runtime>, std::io::Error> {
    let runtime_mutex = RUNTIME.get_or_init(|| Arc::new(Mutex::new(None)));
    let mut runtime_guard = runtime_mutex.lock().unwrap();

    if let Some(ref runtime) = *runtime_guard {
        Ok(runtime.clone())
    } else {
        let runtime = Runtime::new()?;
        let runtime_arc = Arc::new(runtime);
        *runtime_guard = Some(runtime_arc.clone());
        Ok(runtime_arc)
    }
}

/// Clean up the global runtime
pub fn cleanup_runtime() {
    if let Some(runtime_mutex) = RUNTIME.get() {
        let mut runtime_guard = runtime_mutex.lock().unwrap();
        if let Some(runtime_arc) = runtime_guard.take() {
            // Try to get the runtime out of the Arc if it's the only reference
            match Arc::try_unwrap(runtime_arc) {
                Ok(runtime) => runtime.shutdown_background(),
                Err(_arc) => {
                    // If there are other references, we can't shutdown cleanly
                    // This is fine in most cases as the runtime will be cleaned up on process exit
                }
            }
        }
    }
}
