//! Scalar rendering and error identity shared by the CQLite language bindings
//! (issue #1452).
//!
//! # Why this crate exists
//!
//! The byte-level math that turns raw CQL bytes into a displayable scalar —
//! `decimal`, `varint`, `inet` — and the mapping from a core
//! [`cqlite_core::Error`] variant to a binding-visible error identity used to be
//! written **twice**, once in `bindings/python/src` and once in
//! `bindings/node/src`. Two copies drift because nothing forces them to agree;
//! that mechanism produced issues #1450, #1451 and #1453, and it was still live
//! when this crate was created (#1741 and #1754 hardened the two DECIMAL
//! implementations independently and they ended up disagreeing on observable
//! output for inputs both accepted).
//!
//! Every routine here is the **single** implementation of its semantic. Both
//! bindings keep only a thin adapter — bytes → shared function → Py/JS object —
//! and the [`vectors`] tables make "single implementation" an *assertion* rather
//! than a comment: both binding suites render the same committed table through
//! their own production path, so a re-introduced local copy fails both.
//!
//! # Dependency direction — one way only
//!
//! ```text
//! bindings/python (pyo3)  ─┐
//!                          ├─→ cqlite-ffi-common ─→ cqlite-core
//! bindings/node   (napi)  ─┘        (pure Rust)
//! ```
//!
//! This crate depends on `cqlite-core`, `num-bigint` and `serde_json` and
//! **nothing else**. `cqlite-core` does not depend on it, so no cycle is
//! possible. (`serde_json` joined the list in #3505: [`json_number`] classifies
//! a `serde_json::Number`, which is the type both bindings already hold when
//! they convert a `Value::Json` cell. It adds no external code to either
//! binding — both binding manifests already declare it.)
//!
//! # No FFI framework, at any depth
//!
//! `pyo3`, `napi` and `napi-derive` must never appear in this crate's resolved
//! dependency closure — directly, transitively, as a dev-dependency or as a
//! build-dependency. A crate that linked one binding's framework could not be
//! shared with the other, which would defeat the whole point.
//!
//! That rule is **measured, not asserted**: `tests/dependency_boundary.rs`
//! resolves the closure rooted at this package with `cargo metadata` and fails
//! closed on an FFI package, on an empty resolve, and on any measurement it
//! could not take. There is no environment variable that skips it.
//!
//! # A deliberate divergence this crate does NOT unify
//!
//! [`KNOWN_OTEL_KEYS`] is the *name list* both bindings accept for their
//! OpenTelemetry options. The **validation mechanism** is legitimately
//! different and is deliberately left alone: Python receives a `dict` and
//! rejects an unrecognised key with `ValueError`, while napi deserializes a
//! typed object and silently drops unknown JS properties. Do not "fix" that
//! asymmetry by accident — it follows from the two FFI shapes, not from
//! duplicated logic.
//!
//! The list is the **snake_case** spelling (Rust field names / Python dict
//! keys). Node's JS-visible names are the camelCase `#[napi(js_name)]` forms of
//! the same fields.

pub mod decimal;
pub mod error_contract;
pub mod inet;
pub mod json_number;
pub mod otel_keys;
pub mod varint;
pub mod vectors;

pub use decimal::{
    decimal_to_string, DecimalError, DECIMAL_MAX_SCALE_DIGITS, DECIMAL_MAX_UNSCALED_BYTES,
    DECIMAL_POSITIONAL_MAX_BYTES,
};
pub use inet::{inet_bytes_to_string, inet_kind, InetError, InetKind};
pub use json_number::{
    beyond_range_message, beyond_text_to_bigint, beyond_text_to_sign_and_le_words,
    classify_json_number, JsonNumberClass,
};
pub use otel_keys::KNOWN_OTEL_KEYS;
pub use varint::{varint_to_bigint, varint_to_sign_and_le_words};
