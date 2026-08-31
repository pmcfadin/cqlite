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
/// Result-row construction (interned keys #1446 + own-property writes #3630).
mod row;
/// Own-property definition for result rows — the #3630 write mechanism.
mod row_properties;
mod runtime;
mod streaming;
mod value;
// Test-support: the committed cross-binding vector table, rendered through this
// binding's production paths (issue #1452).
//
// `pub` because napi_derive cfg's its `#[napi]` registration out under
// `cfg(test)` (`#[cfg(all(not(test), ...))]` on the generated ctor), so in a
// `cargo test` build of this cdylib nothing inside the module has a Rust caller
// and `dead_code` — denied via `RUSTFLAGS=-D warnings` — would fail the build.
// Public reachability is the honest fix: the surface really is reachable, just
// from JavaScript rather than from Rust.
pub mod vectors;

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

/// The distinct JS error codes the shared FFI error contract can emit
/// (issue #1451) — i.e. the authoritative `node_code` column of
/// `cqlite_ffi_common::error_contract`, sorted and deduplicated.
///
/// Exists so the `ErrorCode` union in `lib/index.d.ts` can be asserted against
/// the TABLE rather than against a hand-written list in the test. Without it,
/// adding a contract row with a new code would ship a code the TypeScript type
/// surface never declares — a silent type lie, and the "same fact written twice,
/// maintained by hand" failure mode. The drift assert lives in
/// `__test__/typescript-definitions.test.js` and fails in BOTH directions.
///
/// Every row is reported regardless of build target (e.g. `Wasm`'s `PLATFORM`),
/// because the union must declare every code the contract can name.
///
/// Not part of the stable public API; `lib/index.js` re-exports it as
/// `_errorContractNodeCodes`.
///
/// @returns Sorted, deduplicated list of JS error codes.
#[napi]
pub fn error_contract_node_codes() -> Vec<String> {
    let mut codes: Vec<String> = cqlite_ffi_common::error_contract::FfiErrorVariant::ALL
        .iter()
        .map(|variant| variant.row().node_code.to_string())
        .collect();
    codes.sort_unstable();
    codes.dedup();
    codes
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
    match cqlite_ffi_common::error_contract::FfiErrorVariant::from_name(&variant)
        .and_then(|v| v.sample_error())
    {
        Some(err) => Err(error::to_napi_error(err)),
        None => Err(error::simple_error(format!(
            "unknown core Error variant '{variant}' (or no representative value \
             for it on this build target)"
        ))),
    }
}
