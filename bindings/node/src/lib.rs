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
mod observability;
mod prepared;
mod refresh;
mod runtime;
mod streaming;
mod value;

pub use database::ColumnInfo;
pub use database::Database;
pub use database::DatabaseOptions;
pub use database::DatabaseStats;
pub use database::MaintenanceOptions;
pub use database::MaintenanceReport;
pub use database::QueryResult;
pub use database::StreamingConfig;
pub use database::WriteStats;
pub use observability::OtelOptions;
pub use prepared::PreparedStatement;
pub use prepared::PreparedStatementStats;
pub use refresh::RefreshReport;
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

/// Test-support: throw the JS error the shared FFI error contract maps a named
/// core `Error` variant to (issue #1451).
///
/// `variant` is a core `cqlite_core::Error` variant identifier, verbatim (e.g.
/// `"CqlParse"`, `"Timeout"`). The probe builds that variant's representative
/// error and returns it through the PRODUCTION `to_napi_error` path — including
/// the `\0code=…\0category=…\0isRecoverable=…` encoding `lib/error-wrapper.js`
/// parses — so the Jest suite can assert `error.code`/`category`/`isRecoverable`
/// for EVERY variant, including `Timeout` and `Memory`, which no query can
/// provoke. This is the Node twin of the Python binding's
/// `_raise_mapped_core_error`, and both read the one shared table, so a
/// cross-binding divergence fails both suites.
///
/// An unrecognized name throws rather than substituting a default row
/// (fail-closed: a typo'd variant must never look like a passing mapping).
/// Not part of the stable public API; `lib/index.js` re-exports it as
/// `_errorContractProbe`.
///
/// @param variant - Core `Error` variant identifier
/// @throws Always: the mapped error for `variant`, or an `INVALID_INPUT` error
///         if the name is unknown.
#[napi]
pub fn error_contract_probe(variant: String) -> napi::Result<()> {
    match cqlite_core::ffi_error_contract::FfiErrorVariant::from_name(&variant)
        .and_then(|v| v.sample_error())
    {
        Some(err) => Err(error::to_napi_error(err)),
        None => Err(error::simple_error(format!(
            "unknown core Error variant '{variant}' (or no representative value \
             for it on this build target)"
        ))),
    }
}
