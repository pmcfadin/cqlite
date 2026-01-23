//! Node.js bindings for CQLite SSTable reader.
//!
//! This crate provides Node.js bindings using napi-rs for reading
//! Apache Cassandra 5.0 SSTables without cluster dependencies.

#![deny(clippy::all)]

use napi_derive::napi;

/// Returns the version of the cqlite-node binding.
#[napi]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Database handle for CQLite SSTable access.
///
/// Placeholder implementation - full API will be implemented in Phase 2.
///
/// ## Thread Safety (Phase 2)
///
/// - Database handles will be thread-safe for use across worker threads
/// - Close operations will be idempotent
/// - Async operations integrate with Node.js event loop via napi's tokio_rt
#[napi]
pub struct Database {
    // TODO(Phase 2): Wrap Arc<cqlite_core::Database> + AtomicBool for thread-safe close
    _private: (),
}

#[napi]
impl Database {
    /// Opens a database at the specified data directory.
    ///
    /// `data_dir` - Path to the SSTable data directory
    ///
    /// Currently throws "Not yet implemented - Phase 2" error.
    #[napi(factory)]
    pub fn open(_data_dir: String) -> napi::Result<Database> {
        Err(napi::Error::from_reason("Not yet implemented - Phase 2"))
    }
}
