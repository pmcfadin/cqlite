//! Python bindings for CQLite SSTable reader.
//!
//! This module provides Python access to CQLite's core functionality
//! for reading Cassandra 5.0 SSTables without cluster dependencies.

use pyo3::prelude::*;

mod config;
mod database;
mod drop_safety;
mod error;
mod observability;
mod prepared;
mod refresh;
mod result;
mod runtime;
mod stats;
mod value;
mod value_hashable;
mod vectors;
mod write;

pub use config::{config_from_py, StreamingConfig};
pub use database::{open, Database};
pub use error::{
    runtime_init_to_py_err, to_py_err, CqliteError, ParseError, QueryError, SchemaError,
};
pub use prepared::PreparedStatement;
pub use refresh::RefreshReport;
pub use result::{ColumnInfo, QueryResult, QueryResultIter, Row, StreamingIterator};
pub use runtime::{block_on, try_get_runtime};
pub use stats::DatabaseStats;
pub use value::{Duration, Udt};
pub use write::{MaintenanceReport, WriteStats};

/// CQLite version string.
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the version of the cqlite library.
#[pyfunction]
fn version() -> &'static str {
    VERSION
}

/// Test-support introspection: reports whether this extension was compiled with
/// the `panic = "abort"` strategy.
///
/// This is read-only and derived at compile time from `cfg!(panic = "abort")`,
/// so it reflects the ACTUAL compiled panic strategy of the loaded wheel (which
/// differs between the debug/test profile, `panic = "unwind"`, and the release
/// profile). The abort-safety harness (`tests/test_abort_safety.py`, issue
/// #1437) keys a conditional strict xfail on this value: under `panic = "unwind"`
/// PyO3 contains a core panic as a catchable exception and the survival cases
/// hard-assert; under `panic = "abort"` the same panic is a process abort until
/// issue #1440 flips the release profile to `panic = "unwind"`.
///
/// Not part of the stable public API; the leading underscore marks it as
/// internal test support.
#[pyfunction]
fn _built_with_panic_abort() -> bool {
    cfg!(panic = "abort")
}

/// Test-support: render a CQL DECIMAL from its raw parts — `scale` and the
/// big-endian two's-complement `unscaled` magnitude — through the exact
/// production conversion (`value::decimal_to_pydecimal`).
///
/// Lets the pytest suite exercise the fail-closed corrupt-DECIMAL guard (issue
/// #1741) directly: a large-but-REPRESENTABLE unscaled value must render as a
/// `decimal.Decimal`, while a genuinely oversized one must raise a typed
/// `CqliteError` (never abort the interpreter) — without needing a multi-kilobyte
/// on-disk fixture. Not part of the stable public API; the leading underscore
/// marks it internal test support.
#[pyfunction]
fn _decimal_from_parts(py: Python<'_>, scale: i32, unscaled: Vec<u8>) -> PyResult<PyObject> {
    value::decimal_to_pydecimal(py, scale, &unscaled)
}

/// Test-support: render a CQL INET from its raw bytes through the exact
/// production conversion (`value::inet_to_py`).
///
/// Lets the pytest suite exercise the malformed-inet typed-error path (issue
/// #1453) directly: 4-byte / 16-byte inputs must render an
/// `ipaddress.IPv4Address` / `IPv6Address`, while any other length must raise a
/// typed CQLite error (matching the Node binding) rather than silently returning
/// raw `bytes` — without needing an on-disk fixture holding a corrupt inet cell.
/// Not part of the stable public API; the leading underscore marks it internal
/// test support.
#[pyfunction]
fn _inet_from_bytes(py: Python<'_>, bytes: Vec<u8>) -> PyResult<PyObject> {
    value::inet_to_py(py, &bytes)
}

/// Test-support: decode a CQL VARINT from its raw big-endian two's-complement
/// bytes through the exact production conversion (`value::varint_to_pyint`).
///
/// The twin of `_inet_from_bytes`/`_decimal_from_parts`, added so the committed
/// cross-binding VARINT vectors (issue #1452) can be driven through the
/// production path without an on-disk fixture per shape. Not part of the stable
/// public API; the leading underscore marks it internal test support.
#[pyfunction]
fn _varint_from_bytes(py: Python<'_>, bytes: Vec<u8>) -> PyResult<PyObject> {
    value::varint_to_pyint(py, &bytes)
}

/// Test-support: convert a JSON number LITERAL to the Python object the
/// production path delivers, through the exact production conversion
/// (`value::value_to_py` on a `Value::Json`).
///
/// The full chain is the one a real result row takes:
/// `value_to_py` → `json_to_py` → `json_number_to_py` →
/// `cqlite_ffi_common::json_number::classify_json_number`. Nothing is
/// re-implemented here, which is the point: without this surface the production
/// adapter had NO test caller at all, so #3505's observable claim — a JSON
/// integer above `i64::MAX` reaches Python as an exact `int`, never a rounded
/// `float` — was asserted by nothing (issue #3505 review round 2).
///
/// `text` is a JSON number literal (`"18446744073709551615"`, `"1.5"`). It is
/// parsed with `serde_json` exactly as the reader would, so the LEXICAL form
/// decides the class — which is the whole subject of #3505. Input that is not a
/// JSON number raises `ValueError` (fail-closed: a typo'd literal must never
/// look like a passing conversion).
///
/// Not part of the stable public API; the leading underscore marks it internal
/// test support.
#[pyfunction]
fn _json_number_from_text(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let number: serde_json::Number = serde_json::from_str(text).map_err(|err| {
        pyo3::exceptions::PyValueError::new_err(format!(
            "`{text}` is not a JSON number literal: {err}"
        ))
    })?;
    value::value_to_py(
        py,
        &cqlite_core::Value::Json(Box::new(serde_json::Value::Number(number))),
    )
}

/// Test-support: raise the Python exception the shared FFI error contract maps a
/// named core `Error` variant to (issue #1451).
///
/// `variant` is a core `cqlite_core::Error` variant identifier, verbatim (e.g.
/// `"CqlParse"`, `"Timeout"`). The probe builds that variant's representative
/// error and returns it through the PRODUCTION `to_py_err` path, so the pytest
/// suite can assert the contract's Python identity for EVERY variant — including
/// `Timeout` and `Memory`, which no test query can provoke. This is the Python
/// twin of the Node binding's `_errorContractProbe`, and both read the one shared
/// table, so a cross-binding divergence is a test failure in both suites.
///
/// An unrecognized name raises `ValueError` rather than substituting a default
/// row (fail-closed: a typo'd variant must never look like a passing mapping).
/// Not part of the stable public API; the leading underscore marks it internal
/// test support.
#[pyfunction]
fn _raise_mapped_core_error(variant: &str) -> PyResult<()> {
    match cqlite_ffi_common::error_contract::FfiErrorVariant::from_name(variant)
        .and_then(|v| v.sample_error())
    {
        Some(err) => Err(to_py_err(err)),
        None => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown core Error variant '{variant}' (or no representative value \
             for it on this build target)"
        ))),
    }
}

/// Python module for CQLite.
#[pymodule]
fn _cqlite(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register version info
    m.add("__version__", VERSION)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;

    // Test-support introspection (issue #1437). See the fn doc comment.
    m.add_function(wrap_pyfunction!(_built_with_panic_abort, m)?)?;

    // Test-support: direct DECIMAL rendering path for the corrupt-guard tests
    // (issue #1741). See the fn doc comment.
    m.add_function(wrap_pyfunction!(_decimal_from_parts, m)?)?;

    // Test-support: direct INET rendering path for the malformed-inet typed-error
    // test (issue #1453). See the fn doc comment.
    m.add_function(wrap_pyfunction!(_inet_from_bytes, m)?)?;

    // Test-support: direct VARINT decoding path for the shared vector table
    // (issue #1452). See the fn doc comment.
    m.add_function(wrap_pyfunction!(_varint_from_bytes, m)?)?;

    // Test-support: direct JSON-number conversion path for the shared vector
    // table (issue #3505). See the fn doc comment.
    m.add_function(wrap_pyfunction!(_json_number_from_text, m)?)?;

    // Test-support: shared FFI error-contract conformance probe (issue #1451).
    // See the fn doc comment.
    m.add_function(wrap_pyfunction!(_raise_mapped_core_error, m)?)?;

    // Test-support: render every committed cross-binding vector through this
    // binding's production paths (issue #1452). See `vectors.rs`.
    m.add_function(wrap_pyfunction!(vectors::_ffi_common_render_vectors, m)?)?;

    // Register exception types
    error::register_exceptions(m)?;

    // Register configuration classes and functions
    config::register_config(m)?;

    // Register database class and open function
    database::register_database(m)?;

    // Register result types (QueryResult, Row, ColumnInfo)
    result::register_result(m)?;

    // Register value-conversion types (exact `Duration`, out-of-band-identity `Udt`)
    value::register_value(m)?;

    // Register prepared statement types
    prepared::register_prepared(m)?;

    // Register refresh report type (issue #1749)
    refresh::register_refresh(m)?;

    // Register database stats types
    stats::register_stats(m)?;

    // Register write-support types (WriteStats, MaintenanceReport)
    write::register_write(m)?;

    Ok(())
}
