//! Tests for the shared FFI error contract table (issue #1451).
//!
//! The table in `cqlite_ffi_common::error_contract` is the single source of truth
//! for how a core `Error` variant surfaces in `bindings/python` and
//! `bindings/node`. These tests pin:
//!
//! 1. **Completeness** — every contract row has a constructible representative
//!    error, and `variant_of` maps that error back to the same row (so a row can
//!    never be orphaned or aliased onto the wrong variant).
//! 2. **Agreement with core** — the row's `category`/`recoverable` copies match
//!    `Error::category()` / `Error::is_recoverable()` for every variant, so the
//!    copy carried for the bindings' single lookup cannot drift.
//! 3. **The rows issue #1451 pins**, including the four cross-binding
//!    divergences it fixes.
//!
//! Note that a *new* core `Error` variant is caught by the compiler, not here:
//! `variant_of` is an exhaustive match over `Error`.

use cqlite_core::error::{Error, ErrorCategory};
use cqlite_ffi_common::error_contract::{
    contract_for, variant_of, FfiErrorRow, FfiErrorVariant, PyExceptionClass,
};

/// The representative error for `v`, failing loudly when one is expected but
/// absent. `None` is legitimate ONLY for `Wasm` off a wasm32 target, where the
/// core variant does not exist.
fn require_sample(v: FfiErrorVariant) -> Option<Error> {
    match v.sample_error() {
        Some(err) => Some(err),
        None => {
            assert_eq!(
                v,
                FfiErrorVariant::Wasm,
                "{:?} has no representative error; only Wasm (off wasm32) may lack one",
                v
            );
            assert!(
                !cfg!(target_arch = "wasm32"),
                "Error::Wasm exists on wasm32, so a sample must be constructible"
            );
            None
        }
    }
}

#[test]
fn every_row_round_trips_through_variant_of() {
    let mut checked = 0usize;
    for &v in FfiErrorVariant::ALL {
        let Some(err) = require_sample(v) else {
            continue;
        };
        assert_eq!(
            variant_of(&err),
            v,
            "variant_of({}) must resolve back to its own contract row",
            v.row().variant
        );
        assert_eq!(contract_for(&err), v.row());
        assert_eq!(
            FfiErrorVariant::from_name(v.row().variant),
            Some(v),
            "row name {} must resolve back to its variant",
            v.row().variant
        );
        checked += 1;
    }
    // Off wasm32 exactly one row (Wasm) has no constructible sample.
    let expected = FfiErrorVariant::ALL.len() - if cfg!(target_arch = "wasm32") { 0 } else { 1 };
    assert_eq!(
        checked, expected,
        "every contract row except Wasm (off wasm32) must be exercised"
    );
}

#[test]
fn table_agrees_with_core_category_and_recoverable() {
    for &v in FfiErrorVariant::ALL {
        let Some(err) = require_sample(v) else {
            continue;
        };
        let row = v.row();
        // The row carries `category`/`recoverable` so a binding needs ONE
        // lookup. They are copies of core's values, and this pins them: a
        // DELIBERATE future divergence must edit this test with its reason.
        assert_eq!(
            row.category,
            err.category(),
            "row {} category must match Error::category()",
            row.variant
        );
        assert_eq!(
            row.recoverable,
            err.is_recoverable(),
            "row {} recoverable must match Error::is_recoverable()",
            row.variant
        );
    }
}

#[test]
fn every_row_has_a_nonempty_screaming_snake_node_code() {
    for &v in FfiErrorVariant::ALL {
        let code = v.row().node_code;
        assert!(
            !code.is_empty(),
            "row {} has an empty code",
            v.row().variant
        );
        assert!(
            code.chars().all(|c| c.is_ascii_uppercase() || c == '_'),
            "node code {code} must be SCREAMING_SNAKE_CASE (it is a JS `error.code`)"
        );
    }
}

#[test]
fn from_name_is_fail_closed_for_an_unknown_variant() {
    assert_eq!(FfiErrorVariant::from_name("NoSuchVariant"), None);
    assert_eq!(FfiErrorVariant::from_name(""), None);
    // Case-sensitive: the name is the core `Error` identifier, verbatim.
    assert_eq!(FfiErrorVariant::from_name("timeout"), None);
    assert_eq!(
        FfiErrorVariant::from_name("Timeout"),
        Some(FfiErrorVariant::Timeout)
    );
}

/// Issue #1695: a query budget elapse must land on the SAME identity as its sibling
/// `Timeout` on both surfaces — Python's builtin `TimeoutError` and node code
/// `TIMEOUT` — so `except TimeoutError:` in Python and a `TIMEOUT` check in JS both
/// catch it. Its core CATEGORY is deliberately `Query` (a budget elapse is a
/// query-execution failure, distinguishable from corruption per #1695), which is a
/// different axis from the surface identity; `table_agrees_with_core_category_and_recoverable`
/// is what keeps that field honest against `Error::classify()`.
#[test]
fn query_timeout_shares_the_timeout_identity_on_both_surfaces() {
    let row = contract_for(
        &FfiErrorVariant::QueryTimeout
            .sample_error()
            .expect("sample exists"),
    );
    assert_eq!(row.py_class, PyExceptionClass::Timeout);
    assert_eq!(row.node_code, "TIMEOUT");
    assert_eq!(
        row.category,
        cqlite_core::error::ErrorCategory::Query,
        "the classify category is a separate axis from the surface identity"
    );
}

/// The row count is pinned so adding an `Error` variant (and therefore a row)
/// is a deliberate, reviewed act rather than an invisible one. Update the count
/// together with the table.
#[test]
fn row_count_is_pinned() {
    assert_eq!(
        FfiErrorVariant::ALL.len(),
        // 39 since issue #3723 added `FixedWidthLengthMismatch`.
        39,
        "contract row count changed — review the new row's py_class/node_code \
         and update this pin"
    );
}

fn row_of(err: &Error) -> FfiErrorRow {
    contract_for(err)
}

#[test]
fn cql_parse_row_is_parse_error_and_real_parse_code() {
    let row = row_of(&Error::cql_parse("bad syntax"));
    assert_eq!(row.variant, "CqlParse");
    assert_eq!(row.py_class, PyExceptionClass::Parse);
    // Issue #1451: Node reported "QUERY" here, because it derived the code from
    // category(). `PARSE` is now a real CQL parse failure in BOTH bindings.
    assert_eq!(row.node_code, "PARSE");
    assert_eq!(row.category, ErrorCategory::Query);
    assert!(!row.recoverable);
    assert_eq!(row.message_prefix, Some("ParseError"));
}

#[test]
fn invalid_input_row_is_value_error_and_not_the_parse_code() {
    let row = row_of(&Error::invalid_input("bad argument"));
    assert_eq!(row.variant, "InvalidInput");
    assert_eq!(row.py_class, PyExceptionClass::Value);
    // Issue #1451: Node reported "PARSE" (from the Data category), which belongs
    // to a genuine CQL parse failure. Bad caller input is INVALID_INPUT.
    assert_eq!(row.node_code, "INVALID_INPUT");
    assert_eq!(row.category, ErrorCategory::Data);
    assert_eq!(row.message_prefix, Some("ValueError"));
}

#[test]
fn timeout_row_has_a_dedicated_timeout_code() {
    let row = row_of(&Error::Timeout("deadline exceeded".to_string()));
    assert_eq!(row.variant, "Timeout");
    assert_eq!(row.py_class, PyExceptionClass::Timeout);
    // Issue #1451: Node collapsed Timeout into "IO" via the System category.
    assert_eq!(row.node_code, "TIMEOUT");
    assert_eq!(row.category, ErrorCategory::System);
    assert!(!row.recoverable);
    assert_eq!(row.message_prefix, Some("TimeoutError"));
}

#[test]
fn memory_row_has_a_dedicated_memory_code() {
    let row = row_of(&Error::memory("allocation failed"));
    assert_eq!(row.variant, "Memory");
    assert_eq!(row.py_class, PyExceptionClass::Memory);
    // Issue #1451: Node collapsed Memory into "IO" via the System category.
    assert_eq!(row.node_code, "MEMORY");
    assert_eq!(row.category, ErrorCategory::System);
    assert!(row.recoverable);
    assert_eq!(row.message_prefix, Some("MemoryError"));
}

#[test]
fn corruption_row_stays_on_the_base_python_class() {
    let row = row_of(&Error::corruption("torn page"));
    assert_eq!(row.variant, "Corruption");
    // Python has no closer builtin/custom class for corrupt on-disk data, so the
    // base class is authoritative here (issue #1451's divergence table).
    assert_eq!(row.py_class, PyExceptionClass::Cqlite);
    assert_eq!(row.node_code, "PARSE");
    assert_eq!(row.category, ErrorCategory::Data);
    assert!(!row.recoverable);
}

#[test]
fn timeout_and_memory_no_longer_share_the_io_identity() {
    let io = row_of(&Error::Io(std::io::Error::other("disk gone")));
    let timeout = row_of(&Error::Timeout("deadline exceeded".to_string()));
    let memory = row_of(&Error::memory("allocation failed"));
    assert_eq!(io.node_code, "IO");
    assert_ne!(timeout.node_code, io.node_code);
    assert_ne!(memory.node_code, io.node_code);
    assert_ne!(timeout.node_code, memory.node_code);
}

#[test]
fn python_class_names_are_the_names_python_sees() {
    assert_eq!(PyExceptionClass::Io.as_str(), "IOError");
    assert_eq!(PyExceptionClass::Parse.as_str(), "ParseError");
    assert_eq!(PyExceptionClass::Value.as_str(), "ValueError");
    assert_eq!(PyExceptionClass::Timeout.as_str(), "TimeoutError");
    assert_eq!(PyExceptionClass::Memory.as_str(), "MemoryError");
    assert_eq!(PyExceptionClass::Cqlite.as_str(), "CqliteError");
    assert_eq!(PyExceptionClass::Cancelled.as_str(), "CancelledError");
}
