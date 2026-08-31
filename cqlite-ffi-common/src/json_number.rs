//! The single implementation of "what host type does a JSON number map to?"
//! (issue #3505).
//!
//! # The defect this module exists to remove
//!
//! Both bindings used to classify a JSON number inline, by fallthrough, and both
//! got it wrong in the same place:
//!
//! ```text
//! if let Some(i) = n.as_i64()      { integer }
//! else if let Some(f) = n.as_f64() { float }        // <-- LOSSY
//! else                             { fallback }     // <-- unreachable
//! ```
//!
//! `18446744073709551615` (`u64::MAX`) is legal JSON. `as_i64()` returns `None`
//! for it, and `as_f64()` then **succeeds lossily** — a `u64 → f64` conversion
//! has 53 mantissa bits — so the value arrived in the host language as
//! `1.8446744073709552e19`, silently. The `else` arm never ran, because without
//! `arbitrary_precision` at least one of `as_i64`/`as_f64` always succeeds.
//!
//! # The order is load-bearing — do not reorder these arms
//!
//! Without the `arbitrary_precision` feature, `serde_json::Number` is exactly
//! one of three variants:
//!
//! ```text
//! N::PosInt(u64) | N::NegInt(i64) | N::Float(f64)
//! ```
//!
//! and the accessors are total on their own variant:
//! `as_i64()` answers for `NegInt` and for a `PosInt` that fits; `as_u64()`
//! answers for every `PosInt`; `as_f64()` answers for `Float`.
//!
//! **Adding the `as_u64()` arm is the whole fix**, because it means the
//! `as_f64()` arm is only ever reached for the `Float` variant — where
//! `as_f64()` is an `f64 → f64` identity and therefore EXACT **with respect to
//! the parsed `Number`**, which is NOT the same as exact with respect to the
//! JSON text: if the text held an integer literal outside
//! `[i64::MIN, u64::MAX]`, `serde_json`'s parser already rounded it into the
//! `Float` variant before any CQLite code ran, and the digits are gone one
//! crate away from here (see *`Beyond` is CORRECT but currently UNREACHABLE*
//! below, which measures exactly that). The claim in this paragraph is about
//! the accessor never *adding* loss, never about the text being recoverable.
//! Move `as_f64()`
//! above `as_u64()` and the `u64`-range integers silently start rounding again,
//! with no compiler complaint and no failing type check. That is why this is
//! written down here rather than left to be re-derived.
//!
//! Measured against `serde_json` 1.0 (`cargo test -p cqlite-ffi-common`, and
//! pinned by `json_number_tests.rs`):
//!
//! | JSON text | `as_i64` | `as_u64` | `as_f64` | class |
//! |---|---|---|---|---|
//! | `9223372036854775807` (`i64::MAX`) | `Some` | `Some` | `9.223372036854776e18` | `I64` |
//! | `9223372036854775808` (`i64::MAX + 1`) | `None` | `Some` | `9.223372036854776e18` | `U64` |
//! | `18446744073709551615` (`u64::MAX`) | `None` | `Some` | `1.8446744073709552e19` | `U64` |
//! | `1e19` (a float LITERAL) | `None` | `None` | `1e19` | `F64` |
//!
//! Note the last two rows together: the JSON **lexical form** decides, not the
//! numeric value. `18446744073709551615` is an integer literal and stays exact;
//! `1.8446744073709552e19` is a float literal and is an `f64` by construction.
//!
//! # `Beyond` is CORRECT but currently UNREACHABLE — stated plainly
//!
//! [`JsonNumberClass::Beyond`] cannot be produced by this build, and no test
//! pretends otherwise. The reason is that the loss happens in `serde_json`'s
//! **parser**, before any CQLite code runs: on integer overflow the parser falls
//! back to `f64`. Measured:
//!
//! * `18446744073709551616` (`u64::MAX + 1`) parses to `Float(1.8446744073709552e19)`
//! * `-9223372036854775809` (`i64::MIN - 1`) parses to `Float(-9.223372036854776e18)`
//! * `123456789012345678901234567890` parses to `Float(1.2345678901234568e29)`
//! * `1e400` does not parse at all (`number out of range`)
//!
//! So the residual — a value outside `[i64::MIN, u64::MAX]` — is already an
//! `f64` when it reaches us, and **no binding-side change can recover it**. The
//! arm is kept because it is the honest shape of the classification, and
//! because keeping it means neither binding has an `_ => lossy fallback` branch
//! to regress into. `json_number_tests.rs` asserts the unreachability rather
//! than faking a `Beyond` case.
//!
//! **`arbitrary_precision` does NOT make this arm carry integers**, and an
//! earlier draft of these docs claimed it did. Verified against
//! `serde_json` 1.0.151 `src/number.rs`: with the feature on, `N = String` and
//! every accessor re-parses that string —
//!
//! ```text
//! pub fn as_f64(&self) -> Option<f64> {
//!     #[cfg(feature = "arbitrary_precision")]
//!     self.n.parse::<f64>().ok().filter(|float| float.is_finite())
//! }
//! ```
//!
//! — so `"123456789012345678901234567890"` still answers
//! `Some(1.2345678901234568e29)` from the `as_f64()` arm and still classifies
//! `F64`, **lossily**, through exactly the arm the fix above was about.
//! `Beyond` becomes reachable only for text OUTSIDE `f64` range (`"1e400"`,
//! which the arbitrary-precision parser no longer rejects up front) — and for
//! that input [`beyond_text_to_bigint`] returns `None`, i.e. a refusal, not an
//! exact integer.
//!
//! Recovering exact digits would therefore need MORE than a feature flag: an
//! exact-integer parse of `Number::as_str()` attempted **before** `as_f64()`.
//! That is the same arm-order lesson as above, one level up — the ordering, not
//! the feature, is what decides whether precision survives.
//!
//! # Why this lives in `cqlite-ffi-common` and not in either binding
//!
//! Two reasons, and the second is the decisive one.
//!
//! First, the classification was written twice and **disagreed with itself**:
//! Python fell through to a lossy `f64` and then to a host-type-shifting
//! `str`, while Node fell through to a lossy `f64` and then to a *fabricated
//! `null`*. One semantic, one implementation — the same rule the `decimal`,
//! `varint` and `inet` modules here exist for.
//!
//! Second, **a unit test in a binding would execute nowhere.** Per
//! `scripts/tests/workspace-test-disposition.txt`, `cqlite-py` is
//! `NOT-EXECUTED` for its Rust half and structurally so: a pyo3 `cdylib`'s
//! `cargo test` harness cannot link libpython. `cqlite-ffi-common` is
//! `EXECUTED / no-gap` — ALL targets, gate component `binding-rust-tests`
//! (#3522). Putting the decision where its tests actually run is the
//! difference between covering this and appearing to.
//!
//! # AC6: `serde_json`'s `arbitrary_precision` is deliberately NOT enabled
//!
//! Issue #3505 asks for a decision rather than a fallthrough. The decision is
//! **no**, for three reasons:
//!
//! 1. **It cannot be scoped to the bindings**, and this workspace already
//!    DEMONSTRATES the mechanism rather than merely being subject to it: the
//!    root `Cargo.toml`'s `[workspace.dependencies]` declares
//!    `serde_json = { version = "1.0", features = ["preserve_order"] }`, and
//!    every member — `cqlite-core`, `cqlite-cli`, both bindings, and this crate
//!    — takes it as `{ workspace = true }`. `arbitrary_precision` would be the
//!    same lever `preserve_order` is already pulled with, applying to all 14
//!    members at once. There is no per-crate seam to hide it behind.
//! 2. **It changes `Number`'s `Serialize` impl** to emit a private
//!    `$serde_json::private::Number` marker token. Serializing a
//!    `serde_json::Value` through any serializer that is not `serde_json`'s own
//!    then breaks — a real hazard on this repository's Parquet/Arrow/Flight
//!    paths, which do exactly that.
//! 3. **It would not buy the fix.** The only residual it addresses is beyond
//!    `u64::MAX` / below `i64::MIN`, which the parser has already collapsed to
//!    `f64` (measured above). The reachable defect — the `u64` range — is fixed
//!    here without it.
//!
//! Reason 3 is the load-bearing one, and it is worth stating what it rules out:
//! **turning the feature on would not even make `Beyond` carry the digits.** As
//! measured in the section above, `as_f64()` under `arbitrary_precision`
//! re-parses the stored text and still answers `Some(_)` for any value inside
//! `f64` range, so an over-`u64` integer literal still classifies `F64` lossily.
//! If that trade is ever revisited, the work is an exact-integer parse of
//! `Number::as_str()` placed BEFORE the `as_f64()` arm — the feature flag alone
//! buys nothing here.

use num_bigint::{BigInt, Sign};

/// The host-representable class of a JSON number.
///
/// `Beyond` **owns** its text rather than borrowing from the `Number`:
/// `serde_json::Number::as_str()` exists only under `arbitrary_precision`, so
/// there is nothing to borrow in a default build and the only way to recover the
/// digits is `Number::to_string()`. The allocation happens exclusively in the
/// arm that is currently unreachable, so the hot paths (`I64`/`U64`/`F64`)
/// allocate nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum JsonNumberClass {
    /// Fits `i64`. Every host language in scope has an exact type for this.
    I64(i64),
    /// Above `i64::MAX`, within `u64`. **This is the class #3505 was losing.**
    U64(u64),
    /// A JSON float literal — or, in a default build, an integer literal the
    /// **parser** already rounded (see the module docs). The `f64` is the exact
    /// parsed value: an `f64 → f64` identity, never a narrowing conversion.
    /// "Exact" is scoped to the parsed `Number`, NOT to the JSON text.
    F64(f64),
    /// Outside all of the above, carrying the raw text. Unreachable without
    /// `arbitrary_precision`; see the module docs.
    Beyond(String),
}

/// Classify a JSON number into the host type that can represent the parsed
/// [`serde_json::Number`] EXACTLY.
///
/// "Exactly" is scoped to the `Number` handed in, not to the JSON text it came
/// from: an integer literal outside `[i64::MIN, u64::MAX]` was already rounded
/// to an `f64` by the parser, before this function is reachable. See the module
/// docs.
///
/// The arm order (`i64` → `u64` → `f64` → text) is load-bearing; the module
/// docs explain why, and reordering it silently reintroduces #3505.
pub fn classify_json_number(n: &serde_json::Number) -> JsonNumberClass {
    if let Some(i) = n.as_i64() {
        JsonNumberClass::I64(i)
    } else if let Some(u) = n.as_u64() {
        // Reached only for a `PosInt` above `i64::MAX`. Before #3505 this arm
        // did not exist and control fell into `as_f64()`, which rounded.
        JsonNumberClass::U64(u)
    } else if let Some(f) = n.as_f64() {
        // Reached only for the `Float` variant, where this is exact.
        JsonNumberClass::F64(f)
    } else {
        JsonNumberClass::Beyond(n.to_string())
    }
}

/// Parse a [`JsonNumberClass::Beyond`] payload as an exact integer, if it is one.
///
/// Lives here rather than in a binding so the `Beyond` arm has ONE
/// interpretation across Python and Node, and so it is covered by the crate
/// whose tests actually execute. Returns `None` for anything that is not an
/// exact integer literal (a huge float literal, for instance) — the caller must
/// then refuse the value rather than substitute a lossy one.
pub fn beyond_text_to_bigint(text: &str) -> Option<BigInt> {
    text.parse::<BigInt>().ok()
}

/// The same value as [`beyond_text_to_bigint`], in napi's
/// `create_bigint_from_words(sign_bit, words)` shape.
///
/// Two shapes, one value — the same split the [`super::varint`] module uses and
/// for the same reason: the word form is a **projection** of the `BigInt`
/// (`BigInt::to_u64_digits`), never computed independently, so the Python and
/// Node arms cannot disagree. Zero is reported as `(false, vec![])`, the empty
/// magnitude, which Node-API renders as `0n`.
pub fn beyond_text_to_sign_and_le_words(text: &str) -> Option<(bool, Vec<u64>)> {
    beyond_text_to_bigint(text).map(|big| {
        let (sign, words) = big.to_u64_digits();
        (sign == Sign::Minus, words)
    })
}

/// The ONE spelling of the refusal message for a JSON number no host type can
/// represent exactly.
///
/// Both bindings map this through their own production error path
/// (`to_py_err` / `to_napi_error`) so the refusal carries the single FFI error
/// contract's identity for a data fault, exactly as the DECIMAL and INET
/// adapters do (issues #1451/#1452). Keeping the message here means the two
/// bindings cannot drift into two different texts for one condition.
pub fn beyond_range_message(text: &str) -> String {
    format!(
        "JSON number `{text}` cannot be represented exactly by any supported \
         host type (issue #3505): it is outside i64/u64 and is not an exact \
         integer literal. Refused rather than delivered as a rounded float."
    )
}

// Unit tests live in a sibling file to keep this module small (#1116).
#[cfg(test)]
#[path = "json_number_tests.rs"]
mod tests;
