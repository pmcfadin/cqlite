//! Node.js bindings for CQLite SSTable reader.
//!
//! This crate provides Node.js bindings using napi-rs for reading
//! Apache Cassandra 5.0 SSTables without cluster dependencies.
//!
//! ## Example
//!
//! ```javascript
//! const { Database } = require('@cqlite/node');
//!
//! const db = await Database.open('/path/to/data', {
//!   schema: '/path/to/schema.cql'
//! });
//!
//! const result = await db.execute('SELECT * FROM users LIMIT 10');
//! console.log(`Got ${result.rowCount} rows`);
//!
//! await db.close();
//! ```

#![deny(clippy::all)]

mod database;
mod error;
mod prepared;
mod streaming;
mod value;

pub use database::ColumnInfo;
pub use database::Database;
pub use database::DatabaseOptions;
pub use database::DatabaseStats;
pub use database::QueryResult;
pub use database::StreamingConfig;
pub use prepared::PreparedStatement;
pub use prepared::PreparedStatementStats;
pub use streaming::StreamingResult;

use napi_derive::napi;

/// Returns the version of the cqlite-node binding.
///
/// @returns The semantic version string (e.g., "0.3.0")
///
/// @example
/// ```javascript
/// const { version } = require('@cqlite/node');
/// console.log(`CQLite version: ${version()}`);
/// ```
#[napi]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
